public class TestMain {
    public static void main(String[] args) {
        AMImpl amImpl = new AMImpl();
        amImpl.Attack();
        amImpl.sayHello();

        System.out.println("=======");

        AM am = new AM();
        am.Move();
        am.Say();

        System.out.println("=======");

        Dog dog = new Dog();
        dog.Eat();
        dog.Speak();
        dog.SuperSpeak();
    }
}
