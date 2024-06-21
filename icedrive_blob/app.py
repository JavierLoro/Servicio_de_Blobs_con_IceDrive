"""Authentication service application."""

from typing import List
import threading  # Module for working with threads
import sys  # Module for system-specific parameters and functions
import logging
import Ice
import IceStorm
import IceDrive
import pyperclip #biblioteca para portapapeles

from .blob import BlobService
from .discovery import Discovery
from .delayed_response import BlobQuery


class BlobApp(Ice.Application):
    """Implementation of the Ice.Application for the Authentication service."""
            
    @staticmethod
    def announce(shutdown: threading.Event, publisher: IceDrive.DiscoveryPrx, blob_prx: IceDrive.BlobServicePrx):
        while not shutdown.wait(5):  # Loop until the shutdown event is set, checking every 5 seconds
            publisher.announceBlobService(blob_prx)  # Announce the BlobService
    
    def run(self, args: List[str]) -> int:
        """Execute the code for the BlobApp class."""
        adapter = self.communicator().createObjectAdapter("BlobAdapter")
        adapter.activate()

        property = self.communicator().getProperties()
        
        manejadorEventos = IceStorm.TopicManagerPrx.checkedCast(                 
            self.communicator().propertyToProxy("IceStorm.TopicManager.Proxy")
        )
        
        try:
            discovery_topic = manejadorEventos.retrieve(property.getProperty("DiscoveryTopic"))     
        except IceStorm.NoSuchTopic:                                                                
            discovery_topic = manejadorEventos.create(property.getProperty("DiscoveryTopic"))       
        try:
            blob_query_topic = manejadorEventos.retrieve(property.getProperty("BlobQueryTopic"))    
        except IceStorm.NoSuchTopic:                                                                
            blob_query_topic = manejadorEventos.create(property.getProperty("BlobQueryTopic"))      
        
        discovery_servant = Discovery()                                         
        discovery_prx = adapter.addWithUUID(discovery_servant)                  
        discovery_topic.subscribeAndGetPublisher({}, discovery_prx)             
        
        servant = BlobService(
            IceDrive.BlobQueryPrx.uncheckedCast(blob_query_topic.getPublisher()),  
            discovery_servant  
        )  
        
        servant_prx = adapter.addWithUUID(servant)
        
        logging.info("Nuevo proxy: %s", servant_prx)
        pyperclip.copy(f"./client.py --Ice.Config=../config/data_transfer.config '{servant_prx}'")
        
        
        query_servant = BlobQuery(servant)
        query_prx = adapter.addWithUUID(query_servant)
        blob_query_topic.subscribeAndGetPublisher({}, query_prx)
        
        
        shutdown = threading.Event()  
        publisher = IceDrive.DiscoveryPrx.uncheckedCast(discovery_topic.getPublisher())  
        blob_prx = IceDrive.BlobServicePrx.uncheckedCast(servant_prx)
        threading.Thread(target=BlobApp.announce, args=(shutdown, publisher, blob_prx)).start()

        self.shutdownOnInterrupt()
        self.communicator().waitForShutdown()

        return 0


def main():
    """Handle the icedrive-authentication program."""
    app = BlobApp()
    return app.main(sys.argv)
