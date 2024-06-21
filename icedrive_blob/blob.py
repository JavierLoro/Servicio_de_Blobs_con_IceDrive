"""Module for servants implementations."""
import os
import logging
import Ice
import IceDrive
import hashlib
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
        self.path = os.path.join(os.getcwd(), "SavesBlobs") #ruta persistencia
        
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
        from .delayed_response import BlobQueryResponse
        future = Ice.Future()                        
        response = BlobQueryResponse(future)        
        response_prx = adapter.addWithUUID(response)    
        response_prx = IceDrive.BlobQueryResponsePrx.uncheckedCast(response_prx)
        auxFunc(blob_id, response_prx)  

        try:
            resultado = future.result(5)                           
        except Ice.TimeoutException:
            logging.debug("[BlobService] Timeout otras instancias")
            raise IceDrive.UnknownBlob(blob_id)                     

        adapter.remove(response_prx.ice_getIdentity())              

        return resultado                                            
    
    def link(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id file as linked in some directory."""
        with open(self.blob_links_path, 'r') as file:
            data = json.load(file)
        try:
            data[blob_id]['links'] += 1
            with open(self.blob_links_path, 'w') as file:
                json.dump(data, file, indent=4)
        except KeyError:
            if not current:
                raise IceDrive.UnknownBlob(blob_id)
            BlobService.askOtherInstances(self.query_prx.linkBlob, blob_id, current.adapter)
        logging.debug(f"[BlobService] Añadido link para blob:{blob_id}")
        
    def createLinkBlob(self, blob_id: str):
        
        with open(self.blob_links_path, 'r') as file:
            data = json.load(file)
            
        if blob_id not in data:
            data[blob_id] = {'links': 0}
               
        with open(self.blob_links_path, 'w') as file:
            json.dump(data, file, indent=4)
                    
        


    def unlink(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id as unlinked (removed) from some directory."""
        with open(self.blob_links_path, 'r') as file:
            data = json.load(file)
        
        try:
            data[blob_id]['links'] -= 1
            if data[blob_id]['links'] <= 0:
                del data[blob_id]
                blob_file_path = os.path.join(self.path, f"{blob_id}.bin")
                if os.path.exists(blob_file_path):
                    os.remove(blob_file_path)
        except KeyError:
            if not current:
                raise IceDrive.UnknownBlob(blob_id)
            BlobService.askOtherInstances(self.query_prx.unlinkBlob, blob_id, current.adapter)
        
        with open(self.blob_links_path, 'w') as file:
            json.dump(data, file, indent=4)
        logging.debug(f"[BlobService] Eliminado link para blob: {blob_id}")
        
    def upload(
        self, user: IceDrive.UserPrx, blob: IceDrive.DataTransferPrx, current: Ice.Current = None
    ) -> str:
        """Register a DataTransfer object to upload a file to the service."""
        #if not self.discovery_servant.getAtuhencticationService().verifyUser(user): #comprobacion usuario
            #raise IceDrive.FailedToReadData
        temp_filename = "temp_file.bin"
        sha256_hash = hashlib.sha256()
        path = os.path.join(self.path, temp_filename)
        logging.debug("[BlobService] Subiendo archivo nuevo")
        try:
            with open(path, "wb") as f:
                still_uploading = True
                while still_uploading: 
                    read_data = blob.read(SIZE)
                    sha256_hash.update(read_data)
                    f.write(read_data)
                    still_uploading = len(read_data) == SIZE
            blob.close()    
            
        except IOError:
            raise IceDrive.FailedToReadData()
        blobId = sha256_hash.hexdigest()
        
        try:
            if not current:
                raise IceDrive.UnknownBlob(blobId)
            BlobService.askOtherInstances(self.query_prx.doesBlobExist, blobId, current.adapter)
            os.remove(path)
        except IceDrive.UnknownBlob:
            logging.debug(f"[BlobService] Archivo nuevo creado blob: {blobId}")
            os.rename(path, os.path.join(self.path, blobId))
            self.createLinkBlob(blobId)
            self.link(blobId)
        return blobId

    
    def download(
        self, user: IceDrive.UserPrx, blob_id: str, current: Ice.Current = None
    ) -> IceDrive.DataTransferPrx:
        """Return a DataTransfer objet to enable the client to download the given blob_id."""
        #if user and not self.discovery_servant.getAtuhencticationService().verifyUser(user):
            #raise IceDrive.FailedToReadData
        logging.debug(f"[BlobService] Intentando descargar blob: {blob_id}")
        path = os.path.join(self.path, blob_id)
        if not os.path.isfile(path):
            if not current:
                raise IceDrive.UnknownBlob(blob_id)
            return BlobService.askOtherInstances(self.query_prx.downloadBlob, blob_id, current.adapter)
                

        
        servant = DataTransfer(os.path.join(self.path, blob_id))
        prx = current.adapter.addWithUUID(servant) if current else None
        logging.debug(f"[BlobService] Creada desscarga blob: {blob_id}, prx: {prx}")

        return IceDrive.DataTransferPrx.uncheckedCast(prx)



        
        
