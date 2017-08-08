# Beta scenario: IBM Analytics Engine
IBM Analytics Engine provides a flexible framework to develop and deploy analytics applications in Apache Hadoop and Apache Spark. It allows you to create and manage clusters using the Bluemix interface or using the Cloud Foundry CLI and REST APIs.

Two scenarios provide tasks that show you how to query data using the Spark SQL through a DSX notebook and how to run a Spark application using Spark submit.

Prior to starting those tasks, you must first create an IBM Analytics Engine cluster and upload data. The tasks are executed on a Spark and Hadoop cluster obtained from IBM Analytics Engine.

## Prerequisites

### Creating an IBM Analytics Engine cluster

1. To create a cluster, log in to [Bluemix](http://www.bluemix.net).

2. Navigate to the Account Management widget and choose the Organization “IBM-CloseBeta” on the top right corner as shown in the image.
![Account Management widget](images/acct-mng-widget.jpg "Account Management widget")

3. Switch the “Space” to the one that was assigned in your welcome email.

4. Then go to the [IBM Analytics Engine catalog page](https://console.bluemix.net/catalog/?env_id=ibm:yp:us-south) to create the cluster.

5. You will find more details on how to create and use the service [here](https://console.bluemix.net/docs/services/AnalyticsEngine/index.html#introduction) in the Bluemix documentation.

### Uploading data
IBM Analytics Engine is based on Apache Hadoop and Spark. While it provides the HDFS file system and a limited amount of storage within the cluster, we recommend using IBM Cloud Object Store or the Swift-based Bluemix Object Storage service to persist data.

Jobs from an Analytics Engine compute cluster can be executed against data in object stores and results of jobs can be sent back to the object store. It also enables you to integrate the storage service with the Watson Data Platform protected access layer, providing the ability to define fine grained access control to your data.

You will find sample data that you can use to get started with the following examples [here](https://github.com/wdp-beta/get-started). This is an open data set from the City of New York containing calls to the 311 number to report issues with infrastructure. We will use the data set in further tutorials.

To upload data into the object store, refer to documentation of the respective offerings: [IBM Cloud Object Store](https://ibm-public-cos.github.io/crs-docs/) / [Bluemix Object Storage service](https://console.bluemix.net/docs/services/ObjectStorage/index.html).

## Scenarios

### Querying data using Spark SQL through a DSX notebook
**Spark SQL** is a **Spark** module for structured data processing. Unlike the basic **Spark** RDD API, the interfaces provided by Spark SQL provide **Spark** with more information about the structure of both the data and the computation being performed. You can interact with **Spark** SQL using SQL and the Dataset API.

For this task, we will use the NYC311 data set that you previously uploaded to your object store. We will also be using DSX Jupyter notebooks to run applications on your IBM Analytics Engine cluster. Before you can start sending queries and jobs using DSX notebooks to the Spark instance in the IBM Analytics Engine service, you need to establish a connection between them.

Within DSX, you can create multiple projects, and each project can have multiple associated services. A provisioned IBM Analytics Engine instance needs to be configured as an associated service with the project that intends to access this IBM Analytics Engine instance.

In addition, a project needs to be already created for you to associate and use IBM Analytics Engine with it. Note that when creating a new project, DSX UI shows a drop down selection box for selecting a Spark Service. The selection made here does not matter. IBM Analytics Engine can only be associated after the project is created.

**To connect your DSX instance to IBM Analytics Engine cluster**

1. Log in to [DSX](https://datascience.ibm.com/).

2. Open the DSX project that you want to use with IBM Analytics Engine.

3. Select the project's **Settings** tab and scroll down to see the **Associated Services** list.

4. Click **add associated service**. A menu of services is displayed.

5. Choose **Amazon EMR Spark**.  A page or form to associate a new service is displayed.<br>
  **Note**: Because the IBM Analytics Engine integration is still under development, DSX currently exposes IBM Analytics Engine support under this option.

6. Enter any name that identifies your IBM Analytics Engine Spark service.

7. For the Personal Access Token, enter `wceadmin:<userid>:<password>`. The `<userid>` and `<password>` are your cluster user and password credentials. To access the credentials, go to **Service** in Bluemix and click the **Credentials** tab.<br>
 **Note**: The "wceadmin:" is an internal identifier that's used by DSX.

8. For the Kernel Gateway URL, enter the notebook_gateway URL present in your cluster VCAP of IBM Analytics Engine. To access the URL, go to **Service** in Bluemix and click the **Credentials** tab.

11. **Save** the URL.<br>
Now when you create new notebooks in this project, you will be able to select this new IBM Analytics Engine Spark Service to run the notebook against.

You are ready to start executing queries and jobs from DSX notebook using the Spark cluster in IBM Analytics Engine.

The iPython notebook [here](https://github.com/wdp-beta/get-started) includes steps and instructions to help you get started to analyze data residing in your object store account using SparkSQL. Add the notebook to your project in DSX and execute it.

### Running a simple Spark application using Spark submit
You can run Spark applications locally or distributed across a cluster, either by using an interactive shell or by submitting an application. In this task, you will learn how to submit a batch job (word count in a text file residing on HDFS).

**To submit a batch job**

1. Upload a text file to HDFS in your cluster. You can do this by using either the Files View in Ambari or WebHDFS APIs. The instructions [here](https://console.bluemix.net/docs/services/AnalyticsEngine/Upload-files-to-HDFS.html#uploading-files-to-hdfs) will help you upload files to HDFS.

2. SSH into the cluster.

3. Copy the script wordcount.py provided [here](https://github.com/wdp-beta/get-started)  to /home/wce/clsadmin/.

4. Go to dir /usr/iop/current/spark2-client/bin.

5. Submit the script using the spark-submit command as shown here:<br>
```spark-submit --master yarn --deploy-mode client --executor-memory 1g --name wordcount --conf "spark.app.id=wordcount" /home/wce/clsadmin/wordcount.py hdfs://name_node_host_name:8020/input_file_path 2```

6. Go to **Manage Cluster** in Bluemix and click the **nodes** tab to get the name node host name. It's the host name of the “management-slave1” node type.

7.	Once the program executes, the output will be available at ‘/home/wce/clsadmin/output.txt’ in your local directory of the management node that you are SSH into.

For more information on submitting Spark jobs, refer to [spark-submit](https://console.bluemix.net/docs/services/AnalyticsEngine/wce-cli-ref-spark-submit.html#spark-submit) in the documentation.
