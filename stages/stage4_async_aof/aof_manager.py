import asyncio
import os
from concurrent.futures import Future


class AOFManager:
    def __init__(
        self,
        node: int,
        aof_dir: str,
        fsync_every_flush: bool = False,
        rewrite_min_size_bytes: int = 1024 * 1024,
        rewrite_growth_factor: float = 2.0,
    ):
        self._node = node
        self._fsync_every_flush = fsync_every_flush
        self._rewrite_min_size_bytes = rewrite_min_size_bytes
        self._rewrite_growth_factor = rewrite_growth_factor

        os.makedirs(aof_dir, exist_ok=True)
        self._aof_path = os.path.join(aof_dir, f"appendonly-shard-{node}.aof")

        self._aof_file = open(self._aof_path, "a+", encoding="utf-8")
        self._aof_file.seek(0, os.SEEK_END)

        self._aof_buffer: list[str] = []
        self._rewrite_buffer: list[str] = []
        self._rewrite_in_progress = False
        self._rewrite_base_size = 0
        self._last_rewrite_size = self._current_aof_size()
        self._rewrite_tmp_path: str | None = None
        self._rewrite_pid: int | None = None
        self._rewrite_task: Future | None = None
        self._rewrite_error: str | None = None

    @property
    def aof_path(self) -> str:
        return self._aof_path

    def load(self, storage):
        self._aof_file.flush()
        self._aof_file.seek(0)

        for raw_line in self._aof_file:
            line = raw_line.strip()
            if not line:
                continue
            self._apply_line(storage, line)

        self._aof_file.seek(0, os.SEEK_END)

    def append_set(self, key: str, value: str, expire_ts: int):
        entry = self._serialize_setabs(key, value, expire_ts)
        self._append_entry(entry)

    def append_delete(self, key: str):
        self._append_entry(f"DELETE {key}\n")

    async def flush_once(self, storage):
        if self._aof_buffer:
            batch = self._aof_buffer
            self._aof_buffer = []
            data = "".join(batch)
            await asyncio.to_thread(self._write_blocking, data)

        await self._finalize_rewrite_if_ready()

        if self._should_start_rewrite():
            await self._start_rewrite(storage)

    async def shutdown(self, storage):
        await self.flush_once(storage)

        if self._rewrite_in_progress and self._rewrite_pid is not None:
            await asyncio.to_thread(os.waitpid, self._rewrite_pid, 0)
            await self._finalize_rewrite_if_ready()

        if self._rewrite_in_progress and self._rewrite_task is not None:
            await asyncio.wrap_future(self._rewrite_task)
            await self._finalize_rewrite_if_ready()

        self._aof_file.close()

    def _apply_line(self, storage, line: str):
        tokens = line.split()
        if not tokens:
            return

        command = tokens[0].upper()
        if command == "SETABS" and len(tokens) >= 4:
            storage.restore_with_expiry(tokens[1], tokens[2], int(tokens[3]))
            return

        if command == "DELETE" and len(tokens) >= 2:
            storage.restore_delete(tokens[1])
            return

        if command == "SET" and len(tokens) >= 3:
            storage.restore_with_ttl(tokens[1], tokens[2], ttl_seconds=30000)
            return

        if command == "MSET" and len(tokens) >= 3:
            for key, value in zip(tokens[1::2], tokens[2::2]):
                storage.restore_with_ttl(key, value, ttl_seconds=30000)

    def _append_entry(self, entry: str):
        self._aof_buffer.append(entry)
        if self._rewrite_in_progress:
            self._rewrite_buffer.append(entry)

    def _serialize_setabs(self, key: str, value: str, expire_ts: int) -> str:
        return f"SETABS {key} {value} {expire_ts}\n"

    def _write_blocking(self, data: str):
        self._aof_file.write(data)
        self._aof_file.flush()
        if self._fsync_every_flush:
            os.fsync(self._aof_file.fileno())

    def _current_aof_size(self) -> int:
        try:
            return os.path.getsize(self._aof_path)
        except FileNotFoundError:
            return 0

    def _should_start_rewrite(self) -> bool:
        if self._rewrite_in_progress:
            return False

        current_size = self._current_aof_size()
        if current_size < self._rewrite_min_size_bytes:
            return False

        baseline = max(self._last_rewrite_size, 1)
        return current_size >= int(baseline * self._rewrite_growth_factor)

    async def _start_rewrite(self, storage):
        self._rewrite_in_progress = True
        self._rewrite_error = None
        self._rewrite_base_size = self._current_aof_size()
        self._rewrite_tmp_path = f"{self._aof_path}.rewrite"
        self._rewrite_buffer = []

        if hasattr(os, "fork"):
            pid = os.fork()
            if pid == 0:
                try:
                    self._write_rewrite_snapshot_from_storage(self._rewrite_tmp_path, storage)
                    os._exit(0)
                except BaseException:
                    os._exit(1)

            self._rewrite_pid = pid
            return

        snapshot = storage.snapshot_items()
        loop = asyncio.get_running_loop()
        self._rewrite_task = loop.run_in_executor(
            None,
            self._write_rewrite_snapshot,
            self._rewrite_tmp_path,
            snapshot,
        )

    def _write_rewrite_snapshot(self, tmp_path: str, snapshot: list[tuple[str, str, int]]):
        with open(tmp_path, "w", encoding="utf-8") as tmp_file:
            for key, value, expire_ts in snapshot:
                tmp_file.write(self._serialize_setabs(key, value, expire_ts))
            tmp_file.flush()
            os.fsync(tmp_file.fileno())

    def _write_rewrite_snapshot_from_storage(self, tmp_path: str, storage):
        self._write_rewrite_snapshot(tmp_path, storage.snapshot_items())

    async def _finalize_rewrite_if_ready(self):
        if not self._rewrite_in_progress:
            return

        if self._rewrite_pid is not None:
            pid, status = os.waitpid(self._rewrite_pid, os.WNOHANG)
            if pid == 0:
                return

            self._rewrite_pid = None
            if status != 0:
                self._rewrite_error = f"rewrite child exited with status {status}"
                self._reset_rewrite_state(clean_temp=True)
                return

        elif self._rewrite_task is not None:
            if not self._rewrite_task.done():
                return

            try:
                await asyncio.wrap_future(self._rewrite_task)
            except Exception as exc:
                self._rewrite_error = str(exc)
                self._reset_rewrite_state(clean_temp=True)
                return
        else:
            return

        rewrite_tail = "".join(self._rewrite_buffer)
        tmp_path = self._rewrite_tmp_path

        await asyncio.to_thread(self._finish_rewrite_blocking, tmp_path, rewrite_tail)

        self._last_rewrite_size = self._current_aof_size()
        self._reset_rewrite_state(clean_temp=False)

    def _finish_rewrite_blocking(self, tmp_path: str, rewrite_tail: str):
        with open(tmp_path, "a", encoding="utf-8") as tmp_file:
            if rewrite_tail:
                tmp_file.write(rewrite_tail)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())

        self._aof_file.close()
        os.replace(tmp_path, self._aof_path)
        self._aof_file = open(self._aof_path, "a+", encoding="utf-8")
        self._aof_file.seek(0, os.SEEK_END)

    def _reset_rewrite_state(self, clean_temp: bool):
        if clean_temp and self._rewrite_tmp_path and os.path.exists(self._rewrite_tmp_path):
            os.remove(self._rewrite_tmp_path)

        self._rewrite_in_progress = False
        self._rewrite_base_size = 0
        self._rewrite_tmp_path = None
        self._rewrite_pid = None
        self._rewrite_task = None
        self._rewrite_buffer = []