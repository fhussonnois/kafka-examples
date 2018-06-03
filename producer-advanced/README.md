# Advance producer API

This project contains a demonstration of how to implement a failover mechanism using the Callback interface 
in order to don't loss any record even in case of cluster failure.

Ths project defines the following classes :
 

* `Application` : The main application entrypoint.
* `ProducerService` : Service that encapsulate KafkaProducer instance.
* `service.ProducerFailover` : Default interface used to write records to a failover storage in case of producer exception.
* `service.FileProducerFailover` : Class implementing the ProducerFailover interface that persist records to a local file.

This project does not implement the logic to recover from a hard failure. Records written to disk are not fallback.