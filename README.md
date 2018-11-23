# Contents
- [Lab 1](#lab-1)
  - Access cluster
- [Lab 2](#lab-2) - Getting started with NiFi
  - Consuming the Meetup RSVP stream
  - Extracting JSON elements we are interested in
  - Splitting JSON into smaller fragments
  - Writing JSON to File System
- [Lab 3](#lab-3) - MiNiFi
  - Enable Site2Site in NiFi
  - Designing the MiNiFi Flow
  - Preparing the flow
  - Running MiNiFi
- [Lab 4](#lab-4) - Kafka Basics
  - Creating a topic
  - Producing data
  - Consuming data
- [Lab 5](#lab-5) - Integrating Kafka with NiFi
  - Creating the Kafka topic
  - Adding the Kafka producer processor
  - Verifying the data is flowing
- [Lab 6](#lab-6) - Integrating the Schema Registry
  - Creating the Kafka topic
  - Adding the Meetup Avro Schema
  - Sending Avro data to Kafka

---------------

# Lab 1

## Accessing your Cluster

Credentials will be provided for these services by the instructor:

* SSH
* Ambari

## Use your Cluster

### To connect using Putty from Windows laptop

All you need is located on: D:/HDF_Workshop

- Use putty to connect to your node using the ppk key:
  - Connection > SSH > Auth > Private key for authentication > Browse... > Select `HDF_workshop_key.ppk`
![Image](https://raw.githubusercontent.com/apsaltis/HDF-Workshop/master/putty.png)

- Create a new seession called `hdf-workshop`
   - For the Host Name use: centos@IP_ADDRESS_OF_EC2_NODE
   - Click "Save" on the session page before logging in

![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/putty-session.png)

<!-- 
### To connect from Linux/MacOSX laptop

- SSH into your EC2 node using below steps:
- Right click to download [this pem key](https://raw.githubusercontent.com/apsaltis/HDF-Workshop/master/HDF_workshop_key.pem)  > Save link as > save to Downloads folder
- Copy pem key to ~/.ssh dir and correct permissions
    ```
    cp ~/Downloads/HDF_workshop_key.pem ~/.ssh/
    chmod 400 ~/.ssh/HDF_workshop_key.pem
    ```
 - Login to the ec2 node of the you have been assigned by replacing IP_ADDRESS_OF_EC2_NODE below with EC2 node IP Address (your instructor will provide this)
    ```
     ssh -i  ~/.ssh/HDF_workshop_key.pem centos@IP_ADDRESS_OF_EC2_NODE

    ```

  - To change user to root you can:
    ```
    sudo su -
    ```
-->

#### Login to Ambari

- Login to Ambari web UI by opening http://{YOUR_IP}:8080 and log in with **admin/admin**

- You will see a list of Hadoop components running on your node on the left side of the page
  - They should all show green (ie started) status. If not, start them by Ambari via 'Service Actions' menu for that service

#### NiFi Install

- NiFi is installed at: /usr/hdf/current/nifi



-----------------------------

# Lab 2

In this lab, we will learn how to:
  - Consume the Meetup RSVP stream
  - Extract the JSON elements we are interested in
  - Split the JSON into smaller fragments
  - Write the JSON to the file system


### Consuming RSVP Data

To get started we need to consume the data from the Meetup RSVP stream, extract what we need, splt the content and save it to a file:

#### Goals:
   - Consume Meetup RSVP stream
   - Extract the JSON elements we are interested in
   - Split the JSON into smaller fragments
   - Write the JSON files to disk

 Our final flow for this lab will look like the following:

  ![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/lab1.png)
  A template for this flow can be found [here](https://raw.githubusercontent.com/apsaltis/HDF-Workshop/master/templates/HDF-Workshop_Lab1-Flow.xml)


  - Step 1: Add a ConnectWebSocket processor to the cavas
      - In case you are using a downloaded template, the ControllerService will be prepopulated. You will need to enable the ControllerService. Double-click the processor and follow the arrow next to the JettyWebSocketClient
      - Set WebSocket Client ID to your favorite number.
      - Set ws://stream.meetup.com/2/rsvps for WebSocket URI
  - Step 2: Add an UpdateAttribute procesor
    - Configure it to have a custom property called ``` mime.type ``` with the value of ``` application/json ```
  - Step 3. Add an EvaluateJsonPath processor and configure it as shown below:
  ![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/jsonpath.png)

    The properties to add are:
    ```
    event.name    $.event.event_name

    event.url     $.event.event_url

    group.city    $.group.group_city

    group.state   $.group.group_state

    group.country	$.group.group_country

    group.name		$.group.group_name

    venue.lat		  $.venue.lat

    venue.lon     $.venue.lon

    venue.name		$.venue.venue_name
    ```
  - Step 4: Add a SplitJson processor and configure the JsonPath Expression to be ```$.group.group_topics ```
  - Step 5: Add a ReplaceText processor and configure the Search Value to be ```([{])([\S\s]+)([}])``` and the Replacement Value to be
    ```
         {
        "event_name": "${event.name}",
        "event_url": "${event.url}",
        "venue" : {
        	"lat": "${venue.lat}",
        	"lon": "${venue.lon}",
        	"name": "${venue.name}"
        },
        "group" : {
          "group_city" : "${group.city}",
          "group_country" : "${group.country}",
          "group_name" : "${group.name}",
          "group_state" : "${group.state}",
          $2
         }
      }
      ```
  - Step 6: Add a PutFile processor to the canvas and configure it to write the data out to ```/tmp/rsvp-data```

##### Questions to Answer
1. What does a full RSVP Json object look like?
2. How many output files do you end up with?
3. How can you change the file name that Json is saved as from PutFile?
4. Why are we using the Update Attribute processor to add a mime.type?
5. How can you cange the flow to get the member photo from the Json and download it.


------------------

# Lab 3

  ![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/lab3.png)
  A template for this flow can be found [here](https://raw.githubusercontent.com/apsaltis/HDF-Workshop/master/templates/MiNiFi_Flow.xml)


## Getting started with MiNiFi ##

In this lab, we will learn how to configure MiNiFi to send data to NiFi:

* Intalling MiNiFi
* Setting up the Flow for NiFi
* Setting up the Flow for MiNiFi
* Preparing the flow for MiNiFi
* Configuring and starting MiNiFi
* Enjoying the data flow!

## Installing MiNiFi
Run all the below commands:
````

sudo mkdir /usr/hdf/current/nifi/minifi
cd /usr/hdf/current/nifi/minifi
sudo wget http://apache.claz.org/nifi/minifi/0.5.0/minifi-0.5.0-bin.tar.gz
sudo wget http://apache.claz.org/nifi/minifi/0.5.0/minifi-toolkit-0.5.0-bin.tar.gz
sudo tar zxvf minifi-0.5.0-bin.tar.gz
sudo tar xzvf minifi-toolkit-0.5.0-bin.tar.gz
  ````

## Setting up the Flow for NiFi
**NOTE:** Before starting NiFi we need to enable Site-to-Site communication. To do that we will use Ambari to update the required configuration. In Ambari the below property values can be found at ````http://<EC2_NODE>:8080/#/main/services/NIFI/configs```` .

* Change:
  ````
			nifi.remote.input.socket.port=

  ````
  To
  ```
   		nifi.remote.input.socket.port=10000

  ```
* Restart NiFi via Ambari


Now we should be ready to create our flow. To do this do the following:

1.	The first thing we are going to do is setup an Input Port. This is the port that MiNiFi will be sending data to. To do this drag the Input Port icon to the canvas and call it "From MiNiFi".

2. Now that the Input Port is configured we need to have somewhere for the data to go once we receive it. In this case we will keep it very simple and just log the attributes. To do this drag the Processor icon to the canvas and choose the LogAttribute processor.

3.	Now that we have the input port and the processor to handle our data, we need to connect them.

4.  We are now ready to build the MiNiFi side of the flow. To do this do the following:
	* Add a GenerateFlowFile processor to the canvas (don't forget to configure the properties on it)
	* Add a Remote Processor Group to the canvas

           For the URL copy and paste the URL for the NiFi UI from your browser
   * Connect the GenerateFlowFile to the Remote Process Group

5. The next step is to generate the flow we need for MiNiFi. To do this do the following steps:

   * Create a template for MiNiFi
   * Select the GenerateFlowFile, the NiFi Flow Remote Processor Group, and the Connection between them (these are the only things needed for MiNiFi)
   * Select the "Create Template" button from the toolbar
   * Choose the name "minifi_flow" for your template


7. Now we need to download the template
8. Now SCP the template you downloaded to the ````/tmp```` directory on your EC2 instance. If you are using Windows you will need to download WinSCP (https://winscp.net/eng/download.php)
9.  We are now ready to setup MiNiFi. However before doing that we need to convert the template to YAML format which MiNiFi uses. To do this we need to do the following:

    * Navigate to the minifi-toolkit directory (/usr/hdf/current/nifi/minifi/minifi-toolkit-0.5.0)
    * Transform the template that we downloaded using the following command:

      ````sudo bin/config.sh transform <INPUT_TEMPLATE> <OUTPUT_FILE>````

      For example:

      ````sudo bin/config.sh transform /tmp/minifi_flow.xml config.yml````

10. Next copy the ````config.yml```` to the ````minifi-0.5.0/conf```` directory. That is the file that MiNiFi uses to generate the nifi.properties file and the flow.xml for MiNiFi.

   ```
   sudo mv config.yml /usr/hdf/current/nifi/minifi/minifi-0.5.0/conf
   ```

11. That is it, we are now ready to start MiNiFi. To start MiNiFi from a command prompt execute the following:

  ```
  cd /usr/hdf/current/nifi/minifi/minifi-0.5.0
  sudo bin/minifi.sh start
  tail -f logs/minifi-app.log
  ```

You should be able to now go to your NiFi flow and see data coming in from MiNiFi.

If you see error logs such as "the remote instance indicates that the port is not in a valid state",
it is because the Input Port has not been started.
Start the port and you will see messages being accumulated in its downstream queue.

------------------

# Lab 4

## Kafka Basics
In this lab we are going to explore creating, writing to and consuming Kafka topics. This will come in handy when we later integrate Kafka with NiFi and Streaming Analytics Manager.

1. Creating a topic
  - Step 1: Open an SSH connection to your EC2 Node.
  - Step 2: Naviagte to the Kafka directory (````/usr/hdf/current/kafka-broker````), this is where Kafka is installed, we will use the utilities located in the bin directory.

    ````
    #cd /usr/hdf/current/kafka-broker/
    ````

  - Step 3: Create a topic using the kafka-topics.sh script
    ````
    bin/kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --replication-factor 1 --topic first-topic

    ````

    NOTE: Based on how Kafka reports metrics topics with a period ('.') or underscore ('_') may collide with metric names and should be avoided. If they cannot be avoided, then you should only use one of them.

  - Step 4:	Ensure the topic was created
    ````
    bin/kafka-topics.sh --list --zookeeper localhost:2181
    ````

2. Testing Producers and Consumers
  - Step 1: Open a second terminal to your EC2 node and navigate to the Kafka directory
  - In one shell window connect a consumer:
  ````
 bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic first-topic
````

    Note: using –from-beginning will tell the broker we want to consume from the first message in the topic. Otherwise it will be from the latest offset.

  - In the second shell window connect a producer:
````
bin/kafka-console-producer.sh --broker-list demo.hortonworks.com:6667 --topic first-topic
````


- Sending messages. Now that the producer is  connected  we can type messages.
  - Type a message in the producer window

- Messages should appear in the consumer window.

- Close the consumer (ctrl-c) and reconnect using the default offset, of latest. You will now see only new messages typed in the producer window.

- As you type messages in the producer window they should appear in the consumer window.

------------------

# Lab 5

## Integrating Kafka with NiFi
1. Creating the topic
  - Step 1: Open an SSH connection to your EC2 Node.
  - Step 2: Naviagte to the Kafka directory (````/usr/hdf/current/kafka-broker````), this is where Kafka is installed, we will use the utilities located in the bin directory.

    ````
    #cd /usr/hdf/current/kafka-broker/
    ````

  - Step 3: Create a topic using the kafka-topics.sh script
    ````
    bin/kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --replication-factor 1 --topic meetup_rsvp_raw

    ````

  - Step 4:	Ensure the topic was created
    ````
    bin/kafka-topics.sh --list --zookeeper localhost:2181
    ````

2. Integrating NiFi
  - Step 1: Add a ````PublishKafka```` processor to the canvas.
  - Step 2: Add a routing for the ````success relationship```` of the ````ReplaceText processor```` to the ````PublishKafka processor```` added in Step 1.
  - Step 3: Configure the topic and broker for the PublishKafka processor, where topic is ````meetup_rsvp_raw```` and broker is ````demo.hortonworks.com:6667````.


3. Start the NiFi flow
4. In a terminal window to your EC2 node and navigate to the Kafka directory and connect a consumer to the ````meetup_rsvp_raw```` topic:

    ````
    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic meetup_rsvp_raw
    ````


5. Messages should now appear in the consumer window.


------------------

# Lab 6

## Integrating the Schema Registry
1. Creating the topic
  - Step 1: Open an SSH connection to your EC2 Node.
  - Step 2: Naviagte to the Kafka directory (````/usr/hdf/current/kafka-broker````), this is where Kafka is installed, we will use the utilities located in the bin directory.

    ````
    #cd /usr/hdf/current/kafka-broker/
    ````

  - Step 3: Create a topic using the kafka-topics.sh script
    ````
    bin/kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --replication-factor 1 --topic meetup_rsvp_avro

    ````

  - Step 4:	Ensure the topic was created
    ````
    bin/kafka-topics.sh --list --zookeeper localhost:2181
    ````

2. Adding the Schema to the Schema Registry
  - Step 1: Open a browser and navigate to the Schema Registry UI. You can get to this from the either the ```Quick Links``` drop down in Ambari, as shown below:

    ![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/registry_quick_link.png)

    or by going to ````http://<EC2_NODE>:17788````
  - Step 2: Create Meetup RSVP Schema in the Schema Registry
    1. Click on “+” button to add new schemas. A window called “Add New Schema” will appear.
    2. Fill in the fields of the ````Add Schema Dialog```` as follows:

        ![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/add_schema_dialog.png)

        For the Schema Text you can download it [here](https://raw.githubusercontent.com/apsaltis/HDF-Workshop/master/meetup_rsvp.asvc) or simply copy and paste it.

        Once the schema information fields have been filled and schema uploaded, click **Save**.

3. We are now ready to integrate the schema with NiFi
  - Step 0: Remove the PutFile and PublishKafka processors from the canvas, we will not need them for this section.
  - Step 1: Add a ````UpdateAttribute```` processor to the canvas.
  - Step 2: Add a routing for the success relationship of the ReplaceText processor to the UpdateAttrbute processor added in Step 1.
  - Step 3: Configure the UpdateAttribute processor as shown below: ``schema.name = meetup_rsvp_avro``

    ![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/update_attribute_schema_name.png)

  - Step 4: Add a ``JoltTransformJSON`` processor to the canvas.
  - Step 5: Add a routing for the ``success relationship`` of the ``UpdateAttribute processor`` to the ``JoltTransformJSON processor`` added in Step 5.
  - Step 6: Configure the JoltTransformJSON processor as shown below:  

    ![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/jolt_transform_config.png)
    The JSON used in the 'Jolt Specification' property is as follows:

    ``
    {
      "venue": {
        "lat": ["=toDouble", 0.0],
        "lon": ["=toDouble", 0.0]
      }
    }
  ``
  - Step 7: Add a ``LogAttribute`` processor to the canvas.
  - Step 8: Add a routing for the ``failure relationship`` of the ``JoltTransformJSON processor`` to the ``LogAttribute processor`` added in Step 7.
  - Step 9: Add a ``PublishKafkaRecord_0_10`` to the canvas.
  - Step 10: Add a routing for the ``success relationship`` of the ``JoltTransformJSON processor`` to the ``PublishKafkaRecord_0_10 processor`` added in Step 9.
  - Step 11: Configure the PublishKafkaRecord_0_10 processor to look like the following: 
  Kafka Brokers : ````demo.hortonworks.com:6667````
  Topic Name:     ````meetup_rsvp_avro````

    ![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/publishkafka_record_configuration.png)


  - Step 12: When you configure the JsonTreeReader and AvroRecordSetWriter, you will first need to configure a schema registry controller service. The schema registry controller service we are going to use is the 'HWX Schema Registry' ````HortonworksSchemaRegistry```` , it should be configured as shown below: ````http://demo.hortonworks.com:7788/api/v1/````

    ![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/hwx_schema_registry_config.png)

  - Step 13: Configure the JsonTreeReader as shown below:

    ![Image](https://github.com/Thiagos25/HDF-Workshop/blob/master/imgs/JsonTreeReader.png)
    
  - Step 14: Configure the AvroRecordSetWriter as shown below:

      ![Image](https://github.com/Thiagos25/HDF-Workshop/blob/master/imgs/AvroRecordSetWriter.png)

    After following the above steps this section of your flow should look like the following:

    ![Image](https://github.com/apsaltis/HDF-Workshop/raw/master/update_jolt_kafka_section.png)


4. Start the NiFi flow
5. In a terminal window to your EC2 node and navigate to the Kafka directory and connect a consumer to the ````meetup_rsvp_avro```` topic:

    ````
    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic meetup_rsvp_avro
    ````


5. Messages should now appear in the consumer window.

