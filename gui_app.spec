# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller specification file for HRS Linkage Tool GUI.

This spec file configures the build of a standalone executable for the
PyQt6-based GUI application.
"""

import sys
import os
from PyInstaller.utils.hooks import collect_data_files, collect_submodules
from PyQt6 import QtCore

block_cipher = None

# Collect all submodules from linkdata package
linkdata_submodules = collect_submodules('linkdata')

# Additional hidden imports for pandas, pyarrow, and PyQt6
hidden_imports = [
    'pandas',
    'pandas._libs.tslibs.timedeltas',
    'pandas._libs.tslibs.nattype',
    'pandas._libs.tslibs.np_datetime',
    'pandas._libs.skiplist',
    'pyarrow.vendored.version',
    'openpyxl',
    'psutil',
    'tqdm',
    'PyQt6.QtCore',
    'PyQt6.QtGui',
    'PyQt6.QtWidgets',
] + linkdata_submodules

# Collect Qt plugins - required for all platforms
# These plugins are essential for Qt to function properly
qt_plugins_path = os.path.join(os.path.dirname(QtCore.__file__), 'Qt6', 'plugins')
datas = [
    (os.path.join(qt_plugins_path, 'platforms'), 'PyQt6/Qt6/plugins/platforms'),
    (os.path.join(qt_plugins_path, 'styles'), 'PyQt6/Qt6/plugins/styles'),
]

a = Analysis(
    ['gui_app.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['scripts/hook-PyQt6.py'],
    excludes=[
        'matplotlib',
        'scipy',
        'numpy.distutils',
        'tkinter',
        # Exclude Qt plugins that cause crashes on macOS
        'PyQt6.QtPositioning',
        'PyQt6.QtLocation',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='HRSLinkageTool',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,  # Disabled: UPX can corrupt Qt DLLs and trigger antivirus false positives
    console=False,  # Set to False for GUI app (no console window)
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,  # Disabled: UPX can corrupt Qt DLLs and trigger antivirus false positives
    upx_exclude=[],
    name='HRSLinkageTool',
)

# macOS app bundle
if sys.platform == 'darwin':
    app = BUNDLE(
        coll,
        name='HRSLinkageTool.app',
        icon=None,  # Add .icns file path here if you have an icon
        bundle_identifier='org.hrsresearch.linkagetool',
        info_plist={
            'CFBundleName': 'HRS Linkage Tool',
            'CFBundleDisplayName': 'HRS Linkage Tool',
            'CFBundleVersion': '0.1.0',
            'CFBundleShortVersionString': '0.1.0',
            'NSHighResolutionCapable': 'True',
            # Privacy keys to prevent Qt from accessing unnecessary services
            'NSLocationWhenInUseUsageDescription': 'This app does not use location services.',
            'NSLocationAlwaysAndWhenInUseUsageDescription': 'This app does not use location services.',
            'NSCameraUsageDescription': 'This app does not use the camera.',
            'NSMicrophoneUsageDescription': 'This app does not use the microphone.',
        },
    )

