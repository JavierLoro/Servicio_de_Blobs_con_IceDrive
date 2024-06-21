"""Servant implementation for the delayed response mechanism."""
import os
import Ice
import IceDrive
import logging


class BlobQueryResponse(IceDrive.BlobQueryResponse):
    """Query response receiver."""
    def __init__(self, future: Ice.Future):
        self.future = future
        
    def downloadBlobResponse(self, blob: IceDrive.DataTransferPrx, current: Ice.Current = None) -> None:
        """Receive a `DataTransfer` when other service instance knows the `blob_id`."""
        self.future.set_result(blob)

    def blobExists(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and it's stored there."""
        self.future.set_result(None)

    def blobLinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was linked."""
        self.future.set_result(None)

    def blobUnlinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was unlinked."""
        self.future.set_result(None)

class BlobQuery(IceDrive.BlobQuery):
    from .blob import BlobService
    """Query receiver."""
    def __init__(self, blob_servant: BlobService) -> None:
        self.blob_servant = blob_servant
        self.path = os.path.join(os.getcwd(), "SavesBlobs")            
    
    def downloadBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query for downloading an archive based on `blob_id`."""
        logging.debug(f"[Servicio blobQuery] Peticion download para el blob {blob_id}")
        if os.path.isfile(os.path.join(self.path, blob_id)):
            logging.debug(f"[Servicio blobQuery] Encontrado el blob {blob_id}")
            response.downloadBlobResponse(self.blob_servant.download(None, blob_id))
            
            """
            servant = self.blob_servant.download(None, blob_id)
            prx = current.adapter.addWithUUID(servant) if current else None
            response.downloadBlobResponse(IceDrive.DataTransferPrx.uncheckedCast(prx))
            """
            
    def doesBlobExist(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to check if a given `blob_id` is stored in the instance."""
        logging.debug(f"[Servicio blobQuery] Peticion blodExist para el blob {blob_id}")
        if os.path.isfile(os.path.join(self.path, blob_id)):
            logging.debug(f"[Servicio blobQuery] Encontrado el blob {blob_id}")
            response.blobExists()

    def linkBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to create a link for `blob_id` archive if it exists."""
        logging.debug(f"[Servicio blobQuery] Peticion link para el blob {blob_id}")
        if os.path.isfile(os.path.join(self.path, blob_id)):
            logging.debug(f"[Servicio blobQuery] Encontrado el blob {blob_id}")
            self.blob_servant.link(blob_id)
            response.blobLinked()

    def unlinkBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to destroy a link for `blob_id` archive if it exists."""
        logging.debug(f"[Servicio blobQuery] Peticion unlink para el blob {blob_id}")
        if os.path.isfile(os.path.join(self.path, blob_id)):
            logging.debug(f"[Servicio blobQuery] Encontrado el blob {blob_id}")
            self.blob_servant.unlink(blob_id)
            response.blobUnlinked()
