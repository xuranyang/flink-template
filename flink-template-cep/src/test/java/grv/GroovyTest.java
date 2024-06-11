package grv;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.io.IOException;

public class GroovyTest {
    public static void main(String[] args) throws IOException {
        Binding binding = new Binding();
        binding.setVariable("age", 20);
        GroovyShell shell = new GroovyShell(binding);

        shell.evaluate("if(age < 18){age_type='未成年'}else{age_type='成年'}");
        String ageType = (String) binding.getVariable("age_type");
        System.out.println(ageType);

        shell.evaluate(new File("flink-template-cep/src/test/java/grv/hello.groovy"));
    }
}
