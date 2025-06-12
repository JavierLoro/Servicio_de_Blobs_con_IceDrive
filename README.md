# IceDrive Blob Service

Proyecto de la asignatura de Sistemas Distribuidos.

Servicio distribuido de almacenamiento de ficheros (blobs) usando ZeroC Ice e IceStorm.

## Requisitos

- Python 3.8 o superior
- ZeroC Ice 3.7 o superior
- IceStorm funcionando

## Uso

1. Ajusta los ficheros de configuración (`blob.config`, `icebox.config`, `icestorm.config`).
2. Inicia el servicio:
   ```bash
   python app.py --Ice.Config=blob.config
