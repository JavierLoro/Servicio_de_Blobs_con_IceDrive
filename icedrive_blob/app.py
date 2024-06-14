"""Authentication service application."""

from typing import List
import threading  # Module for working with threads
import sys  # Module for system-specific parameters and functions

import Ice
import IceStorm
import IceDrive

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
        
        manejadorEventos = IceStorm.TopicManagerPrx.checkedCast(                  #proxy TopicManager
            self.communicator().propertyToProxy("IceStorm.TopicManager.Proxy")
        )
        
        try:
            discovery_topic = manejadorEventos.retrieve(property("DiscoveryTopic"))     # Try to retrieve the topic by name
        except IceStorm.NoSuchTopic:                                            # If the topic does not exist, handle the exception
            discovery_topic = manejadorEventos.create(property("DiscoveryTopic"))       # Create the topic if it does not exist
        try:
            blob_query_topic = manejadorEventos.retrieve(property("BlobQueryTopic"))     # Try to retrieve the topic by name
        except IceStorm.NoSuchTopic:                                            # If the topic does not exist, handle the exception
            blob_query_topic = manejadorEventos.create(property("BlobQueryTopic"))       # Create the topic if it does not exist
        
        discovery_servant = Discovery()                                         # Create an instance of Discovery
        discovery_prx = adapter.addWithUUID(discovery_servant)                  # Add the Discovery servant to the adapter with a UUID
        discovery_topic.subscribeAndGetPublisher({}, discovery_prx)             # Subscribe to the Discovery topic
        
        servant = BlobService(
            IceDrive.BlobQueryPrx.uncheckedCast(blob_query_topic.getPublisher()),  # Get the BlobQuery publisher
            discovery_servant,  # Pass the Discovery servant
            #property("BlobsDirectory"),  # Get the Blobs directory property
            #property("LinksDirectory"),  # Get the Links directory property
            #int(property("DataTransferSize")),  # Get the data transfer size property
            #property("PartialUploadsDirectory")  # Get the partial uploads directory property
        )  # Create an instance of BlobService
        
        servant_prx = adapter.addWithUUID(servant)
        
        print("Proxy: %s", servant_prx)
        
        query_servant = BlobQuery(servant)
        query_prx = adapter.addWithUUID(query_servant)
        blob_query_topic.subscribeAndGetPublisher({}, query_prx)
        
        
        shutdown = threading.Event()  # Create a threading event for shutdown
        publisher = IceDrive.DiscoveryPrx.uncheckedCast(discovery_topic.getPublisher())  # Get the Discovery publisher
        blob_prx = IceDrive.BlobServicePrx.uncheckedCast(servant_prx)  # Get the BlobService proxy
        threading.Thread(target=BlobApp.announce, args=(shutdown, publisher, blob_prx)).start()  # Start the announce thread

        
        #contenidoExtra?
        self.shutdownOnInterrupt()
        self.communicator().waitForShutdown()

        return 0


def main():
    """Handle the icedrive-authentication program."""
    app = BlobApp()
    return app.main(sys.argv)
