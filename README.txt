# DSPS-_Ass1
Authors: Ori Suchy <ID>
        Ohad Abramovitz 307880112

In order to run the application the user needs to change the fields at each file to his personal:

**Main**:bucket name and urls of manager to local sqs & local to manager, manager to worker and worker to manager at each file that uses them.
**Manager**: bucket name and urls of manager to local sqs , local to manager, manager to worker and worker to manager.  
**Worker**: bucket name.

The program uses ami-00e95a9222311e8ed:micro with LabInstanceProfile as IAM profile.

Security:...
Scalability: The program uses Amazon EC2 instances to make a large amount of computations. the Manager receives and processes in parallel all the requests,
and creates a number of instances of Workers to handle operations according to the user's input. This is making the possibility to millions of clients and working on them
simultaneously with millions of workers.
Persistence: If a worker dies, the Manager receives back its message to another worker. Every exception that is thrown within a worker
is sent to the manager which prints it in the output file.
Threads: The Manager uses Threads to process simultaneously requests from clients. Each Thread opens and sends messages to workers, receives the appropriate
messages and creates an output to send to the client.

