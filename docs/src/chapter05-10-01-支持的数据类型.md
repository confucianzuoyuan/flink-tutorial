### 支持的数据类型

Flink支持Java和Scala提供的所有普通数据类型。最常用的数据类型可以做以下分类：

* Primitives（原始数据类型）
* Java和Scala的Tuples（元组）
* Scala的样例类
* POJO类型
* 一些特殊的类型

接下来让我们一探究竟。

*Primitives*

Java和Scala提供的所有原始数据类型都支持，例如`Int`(Java的`Integer`)，String，Double等等。下面举一个例子：

```java
DataStream[Long] numbers = env.fromElements(1L, 2L, 3L, 4L);
numbers.map(n -> n + 1);
```

*Tuples*

元组是一种组合数据类型，由固定数量的元素组成。

Flink为Java的Tuple提供了高效的实现。Flink实现的Java Tuple最多可以有25个元素，根据元素数量的不同，Tuple都被实现成了不同的类：Tuple1，Tuple2，一直到Tuple25。Tuple类是强类型。

```java
DataStream<Tuple2<String, Integer>> persons = env
  .fromElements(
    Tuple2.of("Adam", 17),
    Tuple2.of("Sarah", 23)
  );

persons.filter(p -> p.f1 > 18);
```

Tuple的元素可以通过它们的public属性访问——f0，f1，f2等等。或者使用`getField(int pos)`方法来访问，元素下标从0开始：

```java
import org.apache.flink.api.java.tuple.Tuple2

Tuple2<String, Integer> personTuple = Tuple2.of("Alex", 42);
Integer age = personTuple.getField(1); // age = 42
```

不同于Scala的Tuple，Java的Tuple是可变数据结构，所以Tuple中的元素可以重新进行赋值。重复利用Java的Tuple可以减轻垃圾收集的压力。举个例子：

```java
personTuple.f1 = 42; // set the 2nd field to 42
personTuple.setField(43, 1); // set the 2nd field to 43
```

*POJO*

POJO类的定义：

* 公有类
* 无参数的公有构造器
* 所有的字段都是公有的，可以通过getters和setters访问。
* 所有字段的数据类型都必须是Flink支持的数据类型。

举个例子：

```java
public class Person {
  public String name;
  public int age;

  public Person() {}

  public Person(String name, int age) {
    this.name = name;
    this.age = age;
  }
}

DataStream<Person> persons = env.fromElements(
  new Person("Alex", 42),
  new Person("Wendy", 23)
);
```

*其他数据类型*

* Array, ArrayList, HashMap, Enum
* Hadoop Writable types