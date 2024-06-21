"""Servant implementations for service discovery."""
import logging

import Ice

import IceDrive


class Discovery(IceDrive.Discovery):
    """Servants class for service discovery."""
    def __init__(self):
        self.authproxs = set()   
        self.direcprxs = set() 
        self.blob_proxies = set()  

    def announceAuthentication(self, prx: IceDrive.AuthenticationPrx, current: Ice.Current = None) -> None:
        """Receive an Authentication service announcement."""
        self.authproxs.add(prx)
        logging.info(f"[Discovery] Service annunceAuth {prx}")

    def announceDirectoryService(self, prx: IceDrive.DirectoryServicePrx, current: Ice.Current = None) -> None:
        """Receive an Directory service announcement."""
        self.dir_proxies.add(prx)
        logging.info(f"[Discovery] Service annunceDirectory {prx}")

    def announceBlobService(self, prx: IceDrive.BlobServicePrx, current: Ice.Current = None) -> None:
        """Receive an Blob service announcement."""
        self.blob_proxies.add(prx)
        logging.info(f"[Discovery] Service annunceBlobService {prx}")
        
    def getAuthenticationServices(self, current: Ice.Current = None) -> list[IceDrive.AuthenticationPrx]:
        """Return a list of the discovered Authentication*"""
        return list(self.authproxs)
        
    def getDiscoveryServices(self, current: Ice.Current = None) -> list[IceDrive.DirectoryServicePrx]:
        """Return a list of the discovered DirectoryService*"""
        return list(self.dir_proxies)

    def getBlobServices(self, current: Ice.Current = None) -> list[IceDrive.BlobServicePrx]:
        """Return a list of the discovered BlobService*"""
        return list(self.blob_proxies)
