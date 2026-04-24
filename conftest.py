"""
conftest.py
-----------
Konfigurasi global untuk pytest.

pytest.ini sudah set asyncio_mode = auto, jadi semua test async
berjalan otomatis tanpa perlu decorator @pytest.mark.asyncio.

SOLUSI INISIALISASI:
init_routes() dipanggil di level module src/main.py (bukan di
dalam lifespan), sehingga saat testing tanpa lifespan pun
routes sudah siap. Consumer worker di-start di fixture client().
"""

import os


def pytest_configure(config):
    """Tambah marker asyncio."""
    config.addinivalue_line(
        "markers", "asyncio: mark test as async"
    )