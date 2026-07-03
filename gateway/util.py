"""Atomic file-write helpers hunked from hermes-agent utils.py."""

import errno
import json
import logging
import os
import shutil
import stat
import tempfile
from pathlib import Path
from typing import Any, Union

logger = logging.getLogger(__name__)


def _preserve_file_mode(path: Path) -> "int | None":
    """Capture the permission bits of *path* if it exists, else ``None``."""
    try:
        return stat.S_IMODE(path.stat().st_mode) if path.exists() else None
    except OSError:
        return None


def _restore_file_mode(path: Path, mode: "int | None") -> None:
    """Re-apply *mode* to *path* after an atomic replace.

    ``tempfile.mkstemp`` creates files with 0o600 (owner-only).  After
    ``os.replace`` swaps the temp file into place the target inherits
    those restrictive permissions, breaking Docker / NAS volume mounts
    that rely on broader permissions set by the user.  Calling this
    right after ``os.replace`` restores the original permissions.
    """
    if mode is None:
        return
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def atomic_replace(tmp_path: Union[str, Path], target: Union[str, Path]) -> str:
    """Atomically move *tmp_path* onto *target*, preserving symlinks.

    ``os.replace(tmp, target)`` atomically swaps ``tmp`` into place at
    ``target``.  When ``target`` is a symlink, the symlink itself is
    replaced with a regular file — silently detaching managed deployments
    that symlink ``config.yaml`` / ``SOUL.md`` / ``auth.json`` etc. from
    ``~/.hermes/`` to a git-tracked profile package or dotfiles repo
    (GitHub #16743).

    This helper resolves the symlink first so ``os.replace`` writes to
    the real file in-place while the symlink survives.  For non-symlink
    and non-existent paths the behavior is identical to a plain
    ``os.replace`` call unless the rename fails with ``EXDEV`` or ``EBUSY``;
    those cases fall back to copy/fsync/unlink for cross-device, bind-mount,
    and busy-file deployments.

    Returns the resolved real path used for the replace, so callers that
    need to re-apply permissions can target it instead of the symlink.
    """
    target_str = str(target)
    real_path = os.path.realpath(target_str) if os.path.islink(target_str) else target_str
    tmp_str = str(tmp_path)
    try:
        os.replace(tmp_str, real_path)
    except OSError as exc:
        if exc.errno not in (errno.EXDEV, errno.EBUSY):
            raise
        logger.debug(
            "atomic_replace: %s -> %s failed with %s; falling back to copy",
            tmp_str,
            real_path,
            errno.errorcode.get(exc.errno, exc.errno),
        )
        shutil.copyfile(tmp_str, real_path)
        try:
            shutil.copystat(tmp_str, real_path)
        except OSError:
            pass
        try:
            with open(real_path, "rb") as f:
                os.fsync(f.fileno())
        except OSError:
            pass
        os.unlink(tmp_str)
    return real_path


def atomic_json_write(
    path: Union[str, Path],
    data: Any,
    *,
    indent: int = 2,
    mode: int | None = None,
    **dump_kwargs: Any,
) -> None:
    """Write JSON data to a file atomically.

    Uses temp file + fsync + os.replace to ensure the target file is never
    left in a partially-written state. If the process crashes mid-write,
    the previous version of the file remains intact.

    Args:
        path: Target file path (will be created or overwritten).
        data: JSON-serializable data to write.
        indent: JSON indentation (default 2).
        mode: Optional final permission mode. When set, the temp file is
            created and replaced with this mode, avoiding chmod-after-write
            TOCTOU exposure for secret-bearing files.
        **dump_kwargs: Additional keyword args forwarded to json.dump(), such
            as default=str for non-native types.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    original_mode = None if mode is not None else _preserve_file_mode(path)

    fd, tmp_path = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.stem}_",
        suffix=".tmp",
    )
    try:
        if mode is not None and hasattr(os, "fchmod"):
            # fchmod is Unix-only; Windows' os module has no fchmod. Skipping it
            # here is safe — mkstemp already created the temp file as 0o600, and
            # the post-replace os.chmod below applies the final mode durably.
            os.fchmod(fd, mode)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(
                data,
                f,
                indent=indent,
                ensure_ascii=False,
                **dump_kwargs,
            )
            f.flush()
            os.fsync(f.fileno())
        # Preserve symlinks — swap in-place on the real file (GitHub #16743).
        real_path = atomic_replace(tmp_path, path)
        if mode is not None:
            try:
                os.chmod(real_path, mode)
            except OSError:
                pass
        else:
            _restore_file_mode(Path(real_path), original_mode)
    except BaseException:
        # Intentionally catch BaseException so temp-file cleanup still runs for
        # KeyboardInterrupt/SystemExit before re-raising the original signal.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
