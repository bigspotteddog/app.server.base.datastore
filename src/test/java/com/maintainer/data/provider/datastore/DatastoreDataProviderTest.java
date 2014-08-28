package com.maintainer.data.provider.datastore;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import com.maintainer.util.FieldSortComparator;

public class DatastoreDataProviderTest extends TestCase {

    @Test
    public void test() throws Exception {
        ClassA a1 = new ClassA();
        a1.name = "A";
        a1.b = new ClassB();
        a1.b.name = "B";
        a1.b.c = new ClassC();
        a1.b.c.name = "C";
        a1.b.c.v = 1;

        ClassA a2 = new ClassA();
        a2.name = "B";
        a2.b = new ClassB();
        a2.b.name = "A";
        a2.b.c = new ClassC();
        a2.b.c.name = "D";
        a2.b.c.v = 2;

        ClassA a3 = new ClassA();
        a3.name = "B";
        a3.b = new ClassB();
        a3.b.name = "A";
        a3.b.c = new ClassC();
        a3.b.c.name = "C";
        a3.b.c.v = 3;

        List<ClassA> list = Arrays.asList(a2, a1, a3);
        assertEquals("[B.A.D.2, A.B.C.1, B.A.C.3]", list.toString());

        list = Arrays.asList(a2, a1, a3);
        Collections.sort(list, new FieldSortComparator(ClassA.class, "name"));
        assertEquals("[A.B.C.1, B.A.D.2, B.A.C.3]", list.toString());

        list = Arrays.asList(a2, a1, a3);
        Collections.sort(list, new FieldSortComparator(ClassA.class, "-name"));
        assertEquals("[B.A.D.2, B.A.C.3, A.B.C.1]", list.toString());

        list = Arrays.asList(a2, a1, a3);
        Collections.sort(list, new FieldSortComparator(ClassA.class, "b.c.name"));
        assertEquals("[A.B.C.1, B.A.C.3, B.A.D.2]", list.toString());

        list = Arrays.asList(a2, a1, a3);
        Collections.sort(list, new FieldSortComparator(ClassA.class, "-b.c.name"));
        assertEquals("[B.A.D.2, A.B.C.1, B.A.C.3]", list.toString());

        list = Arrays.asList(a2, a1, a3);
        Collections.sort(list, new FieldSortComparator(ClassA.class, "-name, b.c.name"));
        assertEquals("[B.A.C.3, B.A.D.2, A.B.C.1]", list.toString());

        list = Arrays.asList(a2, a1, a3);
        Collections.sort(list, new FieldSortComparator(ClassA.class, "name, b.c.v"));
        assertEquals("[A.B.C.1, B.A.D.2, B.A.C.3]", list.toString());
    }

    public class ClassA {
        private String name;
        private ClassB b;

        @Override
        public String toString() {
            return name + '.' + b.name + '.' + b.c.name + '.' + b.c.v;
        }
    }

    public class ClassB {
        private String name;
        private ClassC c;
    }

    public class ClassC {
        private String name;
        private int v;
    }
}
