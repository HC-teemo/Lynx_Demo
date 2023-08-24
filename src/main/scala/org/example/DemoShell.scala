package org.example

import org.example.demo1.Demo1
import org.example.demo2.Demo2
import org.jline.utils.{AttributedString, AttributedStyle}
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.shell.jline.PromptProvider
import org.springframework.shell.standard.{ShellComponent, ShellMethod, ShellOption}
import org.springframework.stereotype.Component

@SpringBootApplication
class DemoShellApplication

object DemoShellApplication extends App {
  val demos = List(new Demo1, new Demo2)
  var runner = new DemoRunner(demos(1))
  SpringApplication.run(classOf[DemoShellApplication])
}

@Component class OdcPromptProvider extends PromptProvider {
  def getPrompt: AttributedString = {
    new AttributedString("lynx-demo-shell:>", AttributedStyle.BOLD
        .foreground(AttributedStyle.CYAN).blink())
  }
}

@ShellComponent(value = "lynx demo") class MyCommands {

  @ShellMethod(value="Set Demo.")
  def set(i: Int): Unit = {
    DemoShellApplication.runner = new DemoRunner(DemoShellApplication.demos(i+1))
  }

  @ShellMethod(value="Run cypher.")
  def run(query: String,
          @ShellOption(defaultValue = "false") ast: Boolean,
          @ShellOption(defaultValue = "false") lp: Boolean,
          @ShellOption(defaultValue = "false") pp: Boolean,
          @ShellOption(defaultValue = "20") line: Int): Unit =
    DemoShellApplication.runner.run(query, Map.empty,ast, lp, pp).show(line)

  @ShellMethod(value="Explain cypher.")
  def explain(query: String):Unit =
    DemoShellApplication.runner.run(query, Map.empty, true, true, true)

}

