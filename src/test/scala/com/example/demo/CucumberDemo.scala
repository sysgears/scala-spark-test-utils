package com.example.demo

import com.example.{AdultsDao, SparkAdultsDao, SparkUsersDao, TestJob, UsersDao}
import com.google.inject._
import com.google.inject.name.Named
import com.sysgears.{DataFrames, SparkStub}
import io.cucumber.core.backend.ObjectFactory
import io.cucumber.guice.{CucumberModules, ScenarioScope}
import io.cucumber.java.en.When
import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith

import scala.language.postfixOps

@RunWith(classOf[Cucumber])
@CucumberOptions(
  objectFactory = classOf[TestObjectFactory],
  glue = Array("com.sysgears", "com.example.demo"),
  features = Array("classpath:")
)
class CucumberDemo {}

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

class TestModule extends AbstractModule {
  override def configure(): Unit = {
    bind(classOf[UsersDao]).to(classOf[SparkUsersDao])
    bind(classOf[AdultsDao]).to(classOf[SparkAdultsDao])
  }

  @Provides
  @Singleton
  def dataFrames() = new DataFrames()

  @Provides
  @Singleton
  def session(dataFrames: DataFrames): SparkSession = SparkStub.create(dataFrames)

  @Provides
  @Named("cucumber.spark.datatype-packages")
  def dataTypePackages(): Array[String] = Array("com.example.demo")
}

class Steps @Inject()(job: TestJob) {
  @When("test job is started")
  def runTestJob() = {
    job.run()
  }
}

case class Adults(full_name: String, age: Int)