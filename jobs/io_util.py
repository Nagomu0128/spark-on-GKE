"""_SUCCESS sentinel helpers (Design §8).

Promotion to curated is not atomic on GCS, so readers must trust only partitions
that carry a `_SUCCESS` marker. `publish` writes it; consumers check it.
"""


def _fs_and_path(spark, path):
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs, hpath


def mark_success(spark, partition_dir: str) -> None:
    """Create <partition_dir>/_SUCCESS (overwriting if present)."""
    fs, hpath = _fs_and_path(spark, f"{partition_dir.rstrip('/')}/_SUCCESS")
    fs.create(hpath, True).close()


def success_exists(spark, partition_dir: str) -> bool:
    """True if <partition_dir>/_SUCCESS exists (partition is safe to read)."""
    fs, hpath = _fs_and_path(spark, f"{partition_dir.rstrip('/')}/_SUCCESS")
    return bool(fs.exists(hpath))
