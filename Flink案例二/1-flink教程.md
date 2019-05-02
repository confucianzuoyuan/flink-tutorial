# What can be Streamed?

Flink’s DataStream APIs for Java and Scala will let you stream anything they can serialize. Flink’s own serializer is used for

- basic types, i.e., String, Long, Integer, Boolean, Array
- composite types: Tuples, POJOs, and Scala case classes

and Flink falls back to Kryo for other types.

## Java

### Tuple

For Java, Flink defines its own Tuple1 thru Tuple25 types.

```java
Tuple2<String, Integer> person = new Tuple2<>("Fred", 35);

// zero based index!  
String name = person.f0;
Integer age = person.f1;
```

### POJOs

A POJO (plain old Java object) is any Java class that

- has an empty default constructor
- all fields are either
  - public, or
  - have a default getter and setter


Example:

```java
public class Person {
    public String name;  
    public Integer age;  
    public Person() {};  
    public Person(String name, Integer age) {  
        …  
    };  
}  

Person person = new Person("Fred Flintstone", 35);
```

## Scala tuples and case classes

These work just as you’d expect.

# A Complete Example

This example takes a stream of records about people as input, and filters it to only include the adults.

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;

public class Example {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Person> flintstones = env.fromElements(
				new Person("Fred", 35),
				new Person("Wilma", 35),
				new Person("Pebbles", 2));

		DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {
			@Override
			public boolean filter(Person person) throws Exception {
				return person.age >= 18;
			}
		});

		adults.print();

		env.execute();
	}

	public static class Person {
		public String name;
		public Integer age;
		public Person() {};

		public Person(String name, Integer age) {
			this.name = name;
			this.age = age;
		};

		public String toString() {
			return this.name.toString() + ": age " + this.age.toString();
		};
	}
}
```

## Stream execution environment

Every Flink application needs an execution environment, `env` in this example. Streaming applications should use a `StreamExecutionEnvironment`.

The DataStream API calls made in your application build a job graph that is attached to the `StreamExecutionEnvironment`. When `env.execute()` is called this graph is packaged up and sent to the Job Manager, which parallelizes the job and distributes slices of it to the Task Managers for execution. Each parallel slice of your job will be executed in a task slot.

Note that if you don’t call execute(), your application won’t be run.

![](processes.svg)

This distributed runtime depends on your application being serializable. It also requires that all dependencies are available to each node in the cluster.

## Basic stream sources

In the example above we construct a `DataStream<Person>` using `env.fromElements(...)`. This is a convenient way to throw together a simple stream for use in a prototype or test. There is also a `fromCollection(Collection)` method on `StreamExecutionEnvironment`. We could’ve done this instead:

```java
List<Person> people = new ArrayList<Person>();

people.add(new Person("Fred", 35));
people.add(new Person("Wilma", 35));
people.add(new Person("Pebbles", 2));

DataStream<Person> flintstones = env.fromCollection(people);
```

Another convenient way to get some data into a stream while prototyping is to use a socket

```java
DataStream<String> lines = env.socketTextStream("localhost", 9999)
```

or a file

```java
DataStream<String> lines = env.readTextFile("file:///path");
```

In real applications the most commonly used data sources are those that support low-latency, high throughput parallel reads in combination with rewind and replay – the prerequisites for high performance and fault tolerance – such as Apache Kafka, Kinesis, and various filesystems. REST APIs and databases are also frequently used for stream enrichment.

## Basic stream sinks

The example above uses `adults.print()` to print its results to the task manager logs (which will appear in your IDE’s console, when running in an IDE). This will call `toString()` on each element of the stream.

The output looks something like this

```sh
1> Fred: age 35
2> Wilma: age 35
```

where 1> and 2> indicate which sub-task (i.e., thread) produced the output.

You can also write to a text file

```java
stream.writeAsText("/path/to/file")
```

or a CSV file

```java
stream.writeAsCsv("/path/to/file")
```

or a socket

```java
stream.writeToSocket(host, port, SerializationSchema)
```

In production, commonly used sinks include Kafka as well as various databases and filesystems.

## Debugging

In production you will submit an application JAR file to a remote cluster where your application will run. If it fails, it will also fail remotely. The job manager and task manager logs can be very helpful in debugging such failures, but it’s much easier to do local debugging inside an IDE, which is something that Flink supports. You can set breakpoints, examine local variables, and step through your code. You can also step into Flink’s code, which can be a great way to learn more about its internals if you are curious to see how Flink works.