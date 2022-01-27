public interface IHero {
    public void Attack();

    default void sayHello(){
        System.out.println("hello world");
    }
}
