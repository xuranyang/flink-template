public class Dog extends Animal {

    @Override
    public void Speak() {
//        super.Speak();
        System.out.println("wang wang wang");
    }

    public void SuperSpeak(){
        super.Speak();
    }
}
