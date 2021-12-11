# DSPS-_Ass1
Authors: Ori Suchy 204051114
        Ohad Abramovitz 307880112

In order to run the application the user needs to change the fields at each file to his personal:

**Main**:bucket name and urls of manager to local sqs & local to manager, manager to worker and worker to manager at each file that uses them.
**Manager**: bucket name and urls of manager to local sqs , local to manager, manager to worker and worker to manager.  
**Worker**: bucket name.

The user should also upload Manager.jar and Worker.jar to his s3 bucket.
The program is initiated from the Main.jar file with args: java -jar Main.jar inputFileName outputFileName n [terminate]
n- number of workers per messages (ratio).
terminate - if the word "terminate" is given, this app will close the manager.

The Main class uploads the input file to s3 and sends its s3 URL along with the worker ratio to the local-manager sqs.
Then it checks if a Manager exists. if it isn't, then it creates it.
Then, it waits for a message with output URL from manager to local sqs.
When it receives its message, it saves locally the output file and if "terminate" was put in the args it sends a
"terminate" message to the manager. In any case it ends its run afterwards.

The manager node listens to the local to manager sqs. When it receives a message, if it's a file then it executes a thread to process
the message. If it receives "terminate" message, then it shuts down all the threads and running workers and lastly itself.
The Thread reads the lines of the file, saves them in a list and sends each line to the manager to worker sqs.
Then, if there are missing worker nodes according to the ratio, the Thread opens the needed workers.
The thread listens for messages from the worker to manager sqs. If it has received a message directed to it, it saves the message.
When all the messages were received, it creates html output file and sends it to the manager to local sqs to the suitable client.

The worker node listens to the manager to worker sqs. When a message is received, the worker interprets extract the operation and url
and executes the operation on the url. Then it sends the result back to worker to manager sqs.


The program uses ami-00e95a9222311e8ed:micro with LabInstanceProfile as IAM profile.

Runtime: A local app with input "input-sample-1.txt" and n=350
         A local app with input "input-sample-2.txt" and n=13
         12 minutes (8 workers).


Scalability: The program uses Amazon EC2 instances to make a large amount of computations. the Manager receives and processes in parallel all the requests,
and creates a number of instances of Workers to handle operations according to the user's input. This is making the possibility to millions of clients and working on them
simultaneously with millions of workers. (In this program there is a limit of 15 workers due to amazon limitations of no more than 19 instances)
Persistence: If a worker dies, the Manager receives back its message to another worker. Every exception that is thrown within a worker
is sent to the manager which prints it in the output file.
Threads: The Manager uses Threads to process simultaneously requests from clients. Each Thread opens and sends messages to workers, receives the appropriate
messages and creates an output to send to the client.

