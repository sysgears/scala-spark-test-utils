# Test utils for Apache Spark

This repositories provides some util classes that helps readability of your unit/integration tests for Spark projects.

## DataFrame declaration syntax

Using `com.sysgears.DataFramesBuilder` you can define your DataFrames in tests with slightly more readable way:

```scala
import com.sysgears.DataFrameBuilder._
import org.apache.spark.sql.SparkSession
...

implicit val spark: SparkSession = ...

val users =
    ! "first_name" | "last_name" | "age" |
    ! "John"       | "Johnson"   | 17    |
    ! "Henry"      | "Petrovich" | 18    |
    ! "Harry"      | "Harrison"  | 19    |
```

First row is a header - names of columns. Other rows contains data. Types of data is defined by first row.
Due to unary ! that starts row it has restriction - first column type must not be a boolean. To achieve this, just 
change order of columns when first column is a `Boolean`.

## DataFrames util to store state.

DataFrames class allow you to mock your DAO that provides data frames for next asserting on it. It has two params:
format and path to make it closer to the Spark API.

To create/get DataFrames object:
```scala
import com.sysgears.DataFrames
...

new DataFrames().addReadableTable("jdbc", "users", users) // or
DataFrames.threadLocal.addReadableTable("jdbc", "users", users)
```

Now you can implement your test DAO as next:

```scala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.sysgears.DataFrames

trait UsersDao {
  def getAll(): DataFrame
  def save(dataFrame: DataFrame)
}

class TestUsersDao(dataFrames: DataFrames) extends UsersDao {
  override def getAll: DataFrame = dataFrames.read("jdbc", "users")
  override def save(dataFrame:  DataFrame) = dataFrames.write("jdbc","users", dataFrame)
}
```

After that to get all written `DataFrame`:

```scala
import com.sysgears.DataFrames
...

dataFrames.getWrittenTable("jdbc", "users").show() // or
DataFrames.threadLocal.getWrittenTable("jdbc", "users").show()
```

## Spark stub object

__This feature is experimental__

You can use `SparkStub` class to use it in tests and without any DAO.

```scala
val dataFrames = new DataFrames()
val spark: SparkSession = SparkStub.create(dataFrames)
```

As first argument it takes dataFrames object (which is DataFrames.threadLocal by default). Every `load` operation is
replaced by `dataFrames.read("...", "...")` and every `save` operation on DataSet is replaced by 
`dataFrames.write("...","...", ...)`.
So you can use `dataFrames.addReadableTable("...", "...", ...)` to add tables to be read by spark, and
get all written data by using `dataFrames.getWrittenTable("...", "...")`

## Cucumber

### Converters

To convert Cucumber's DataTable to/from Spark's DataFrame you can use `DataSetConverter` with converter methods.

```scala
import io.cucumber.java.en.Given
import io.cucumber.datatable.DataTable

class Steps {
    private implicit val sparkSession: SparkSession = ...

    @Given("spark format: {string} for table: {string} has data:")
    def setDataFrame(format: String, tableOrPath: String, dataTable: DataTable): Unit = {
      dataTable.asDataFrame()
    }
}
```

After that you will be able to use it like this:
```gherkin
Given spark format: "jdbc" for table: "users" has data:
  | first_name STRING | last_name STRING | age INT |
  | John              | Petrovich        | 17      |
  | Henry             | Johnson          | 18      |
  | Harry             | Potter           | 19      |
```

You can enumerate default fields as Spark SQL:

```scala
dataTable.asDataFrame(defaults = "age INT")
```

You can omit types declaration, if you have some java/scala type that represents this DataFrame:

```scala
dataTable.asTypeCheckedDataFrame(classOf[User])
dataTable.asTypeCheckedDataFrame(classOf[User], defaults = "age")
```

### Predefined steps

Library already has a bunch of steps might be used to write BDD tests. Here is a list of all available given steps:

```gherkin
Given spark format: "jdbc" for table: "users" has data:
  | first_name STRING | last_name STRING | age INT |
  | John              | Petrovich        | 17      |

Given spark format: "jdbc" for table: "users" has data with defaults "age INT":
  | first_name STRING | last_name STRING |
  | John              | Petrovich        |

Given spark format: "jdbc" for table: "users" has data as "com.example.User":
  | first_name | last_name | age |
  | John       | Petrovich | 17  |

Given spark format: "jdbc" for table: "users" has data with defaults "age" as "com.example.User":
  | first_name | last_name |
  | John       | Petrovich |

Given spark format: "jdbc" for table: "users" has data with defaults as "com.example.User":
  | first_name | last_name |
  | John       | Petrovich |
```

and all `then` steps:

```gherkin
Then spark format: "jdbc" for table: "users" has data:
  | first_name STRING | last_name STRING | age INT |
  | John              | Petrovich        | 17      |

Then spark format: "jdbc" for table: "users" wrote data with defaults "age INT":
  | first_name STRING | last_name STRING |
  | John              | Petrovich        |

Then spark format: "jdbc" for table: "users" wrote data as "com.example.User":
  | first_name | last_name | age |
  | John       | Petrovich | 17  |

Then spark format: "jdbc" for table: "users" wrote data with defaults "age" as "com.example.User":
  | first_name | last_name |
  | John       | Petrovich |

Then spark format: "jdbc" for table: "users" wrote data with defaults as "com.example.User":
  | first_name | last_name |
  | John       | Petrovich |
```

`SparkSteps` is integrated with `DataFrames`, so in your test job runner code you should pass it to your job method or 
bind it to your DAO mock or use `SparkStub`.
`SparkSteps` is integrated by `@Inject` from Guice, and takes 3 params:

* `SparkSession` - required to enable conversion between Cucumber's `DataTAale` and Spark's `DataFrame`
* `DataFrames` - required to enable it to add available to read tables and assert on written tables
* `@Named("cucumber.spark.datatype-packages") dataTypesPackages: Array[String]` - 
allows to use short class names in steps. For example, when `dataTypesPackages = Array("com.example", "com")` you can 
write short class names:

```gherkin
Then spark format: "jdbc" for table: "users" wrote data as "User":
Then spark format: "jdbc" for table: "users" wrote data as "example.User":
```

To provide this args, lets create module for Cucumber:

```scala
import com.google.inject._
import com.google.inject.name.Named
import com.sysgears.{DataFrames, SparkStub}
import org.apache.spark.sql.SparkSession
import io.cucumber.core.backend.ObjectFactory

class TestModule extends AbstractModule {
  override def configure(): Unit = {}

  @Provides
  @Singleton
  def dataFrames() = new DataFrames() // or DataFrames.threadLocal

  @Provides
  @Singleton
  def session(dataFrames: DataFrames): SparkSession = SparkStub.create(dataFrames) // or any other session

  @Provides
  @Named("cucumber.spark.datatype-packages")
  def dataTypePackages(): Array[String] = Array("com.example.demo")
}
```

Configure Object factory:

```scala
import com.google.inject._
import io.cucumber.core.backend.ObjectFactory
import io.cucumber.guice.{CucumberModules, ScenarioScope}

class TestObjectFactory extends ObjectFactory {
  private val injector = Guice.createInjector(
    Stage.PRODUCTION,
    CucumberModules.createScenarioModule,
    new TestModule()
  )

  override def start(): Unit = injector.getInstance(classOf[ScenarioScope]).enterScope()
  override def stop(): Unit = injector.getInstance(classOf[ScenarioScope]).exitScope()
  override def getInstance[T](glueClass: Class[T]): T = injector.getInstance(glueClass)
  override def addClass(glueClass: Class[_]): Boolean = true
}
```
`/src/resources/META-INF/services/io.cucumber.core.backend.ObjectFactory`:
```
com.example.demo.TestObjectFactory
```

And eventually pass all args to Cucumber (here we are using junit integration):

```scala
import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
  objectFactory = classOf[TestObjectFactory],       // object factory we created above
  glue = Array("com.sysgears", "com.example.demo"), // com.sysgears contains step definitions, com.example.demo - just for example
  features = Array("classpath:")                    // to load all features defined in the root of resources folder
)
class CucumberDemo {}
```