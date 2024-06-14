"""Module for servants implementations."""
import os
import Ice
import IceDrive
import hashlib
import sys
import json
from typing import Callable, Any

from .discovery import Discovery

SIZE=1024


class DataTransfer(IceDrive.DataTransfer):
    """Implementation of an IceDrive.DataTransfer interface."""
    def __init__(self, ruta_archivo: str):
        self.file = open(ruta_archivo,"rb")

    def read(self, size: int, current: Ice.Current = None) -> bytes:
        """Returns a list of bytes from the opened file."""
        try:
            return self.file.read(size)
        except IOError:
            raise IceDrive.FailedToReadData()
        
    def close(self, current: Ice.Current = None) -> None:
        """Close the currently opened file."""
        self.file.close()
        if current:
            current.adapter.remove(current.id)
            


class BlobService(IceDrive.BlobService):
    """Implementation of an IceDrive.BlobService interface."""
    
    def __init__(self, query_prx: IceDrive.BlobQueryPrx, discovery_servant: Discovery):
        self.query_prx= query_prx
        self.discovery_servant = discovery_servant
        self.path = os.path.join(os.path.dirname(os.getcwd()), "ssdd/blobs") #ruta persistencia
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        
        # Path de BloblsLinks.json
        self.blob_links_path = os.path.join(self.path, "BloblsLinks.json")
        
        # Check if the BloblsLinks.json file exists, and create it if not
        if not os.path.exists(self.blob_links_path):
            with open(self.blob_links_path, 'w') as file:
                json.dump({}, file)  # Initialize with an empty dictionary

    @staticmethod
    def askOtherInstances(auxFunc: Callable[[str, IceDrive.BlobQueryResponsePrx], None], blob_id: str, adapter: Ice.ObjectAdapterI) -> Any:
        """Ask for help to other instances to find a blob_id."""
        future = Ice.Future()                           # Crea un future para la respuesta.
        from .delayed_response import BlobQueryResponse
        response = BlobQueryResponse(future)  # Crea una respuesta diferida.
        response_prx = adapter.addWithUUID(response)  # Añade la respuesta al adaptador con un UUID.
        response_prx = IceDrive.BlobQueryResponsePrx.uncheckedCast(response_prx)  # Castea el proxy de la respuesta.
        auxFunc(blob_id, response_prx)  # Llama a la función de ayuda.

        try:
            resultado = future.result(5)                           # Espera el resultado del futuro con un timeout de 5 segundos.
        except Ice.TimeoutException:
            raise IceDrive.UnknownBlob(blob_id)                     # Lanza una excepción si el futuro expira.

        adapter.remove(response_prx.ice_getIdentity())              # Remueve el proxy del adaptador.

        return resultado                                            # Devuelve el resultado.
    
    def link(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id file as linked in some directory."""
        
        with open(self.blob_links_path, 'r') as file:
            data = json.load(file)
        
        
        # Update the link count for the given blob_id
        try:
            data[blob_id]['links'] += 1
            with open(self.blob_links_path, 'w') as file:
                json.dump(data, file, indent=4)
        except KeyError:
            if not current:
                raise IceDrive.UnknownBlob(blob_id)
            BlobService.askOtherInstances(self.query_prx.linkBlob, blob_id, current.adapter)
        
        # Save the updated data back to the file
        
    def createLinkBlob(self, blob_id: str):
        with open(self.blob_links_path, 'r') as file:
            data = json.load(file)

        data[blob_id] = {'links': 1}
        with open(self.blob_links_path, 'w') as file:
            json.dump(data, file, indent=4)
                    
        


    def unlink(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id as unlinked (removed) from some directory."""
        with open(self.blob_links_path, 'r') as file:
            data = json.load(file)
        
        # Update the link count for the given blob_id
        try:
            data[blob_id]['links'] -= 1
            if data[blob_id]['links'] <= 0:
                del data[blob_id]
                blob_file_path = os.path.join(self.path, f"{blob_id}.bin")
                if os.path.exists(blob_file_path):
                    os.remove(blob_file_path)
        except KeyError:
            if not current:
                raise IceDrive.UnknownBlob(blob_id)  # Lanza una excepción si el blob_id es desconocido.

            BlobService.askOtherInstances(self.query_prx.unlinkBlob, blob_id, current.adapter)
        
        
        # Save the updated data back to the file
        with open(self.blob_links_path, 'w') as file:
            json.dump(data, file, indent=4)
        
    def upload(
        self, user: IceDrive.UserPrx, blob: IceDrive.DataTransferPrx, current: Ice.Current = None
    ) -> str:
        """Register a DataTransfer object to upload a file to the service."""
        if not self.discovery_servant.getAtuhencticationService().verifyUser(user): #comprobacion usuario
            raise IceDrive.FailedToReadData
        
        temp_filename = "temp_file.bin"
        sha256_hash = hashlib.sha256()
        path = os.path.join(self.path, temp_filename)
        
        try:
            with open(path, "wb") as f:
                for byte_block in iter(lambda: blob.read(SIZE), b""):
                    sha256_hash.update(byte_block)
                    f.write(byte_block)
            blob.close()        
            
        except IOError:
            raise IceDrive.FailedToReadData()
        blobId = sha256_hash.hexdigest()
        
        try:
            if not current:
                raise IceDrive.UnknownBlob(blobId)
            BlobService.askOtherInstances(self.query_prx.blobIdExists, blobId, current.adapter)
            os.remove(path)
        except IceDrive.UnknowBlob:
            os.rename(path, blobId)
            self.createLinkBlob(blobId)
        
        return blobId

    
    def download(
        self, user: IceDrive.UserPrx, blob_id: str, current: Ice.Current = None
    ) -> IceDrive.DataTransferPrx:
        """Return a DataTransfer objet to enable the client to download the given blob_id."""
        if user and not self.discovery_servant.getAtuhencticationService().verifyUser(user):
            raise IceDrive.FailedToReadData
        
        blodPath=blob_id + ".bin"
        
        for root, dirs, files in os.walk(self.path):
            if blodPath not in files:
                raise IceDrive.UnknownBlob(blob_id)
            
            return BlobService.ask_for_help(self.query_prx.downloadBlob, blob_id, current.adapter)

        
        servant = DataTransfer(os.path.join(self.path, blob_id))
        prx = current.adapter.addWithUUID(servant) if current else None


        return IceDrive.DataTransferPrx.uncheckedCast(prx) if current else servant

        
        
