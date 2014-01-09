package custom;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class PageInfo
{
    public static void main(String... args)
    throws Exception
    {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        Unsafe unsafe = (Unsafe)f.get(null);

        int pageSize = unsafe.pageSize();
        System.out.println("Page size: " + pageSize);
    }
}