package com.maintainer.data.provider.datastore;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

public class DatastoreDataProviderTest extends TestCase {

    @Test
    public void test() throws Exception {
        ClassA a1 = new ClassA();
        a1.name = "A";
        a1.b = new ClassB();
        a1.b.name = "B";
        a1.b.c = new ClassC();
        a1.b.c.name = "C";

        ClassA a2 = new ClassA();
        a2.name = "B";
        a2.b = new ClassB();
        a2.b.name = "C";
        a2.b.c = new ClassC();
        a2.b.c.name = "D";

        ClassA a3 = new ClassA();
        a3.name = "C";
        a3.b = new ClassB();
        a3.b.name = "D";
        a3.b.c = new ClassC();
        a3.b.c.name = "E";

        List<ClassA> list = Arrays.asList(a2, a1, a3);
        assertEquals("[B, A, C]", list.toString());

        Collections.sort(list, new SortComparator(ClassA.class, "b.c.name"));
        assertEquals("[A, B, C]", list.toString());
    }

    private class SortComparator implements Comparator<Object> {

        private final Class<?> clazz;
        private final String[] sorts;

        public SortComparator(Class<?> clazz, String order) {
            this.clazz = clazz;
            this.sorts = order.replaceAll("^[,\\s]+", "").split("[,\\s]+");
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public int compare(Object o1, Object o2) {
            int compareTo = 0;
            try {
                for (String s : sorts) {
                    List<Field> fields = getFields(clazz, s);
                    Object v1 = getValue(o1, fields);
                    Object v2 = getValue(o2, fields);

                    if (!Comparable.class.isAssignableFrom(v1.getClass()) || !Comparable.class.isAssignableFrom(v2.getClass())) {
                        throw new Exception("Not comparable.");
                    }

                    Comparable c1 = (Comparable) v1;
                    Comparable c2 = (Comparable) v2;

                    compareTo = c1.compareTo(c2);
                    if (compareTo != 0) {
                        break;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return compareTo;
        }

        private List<Field> getFields(Class<?> clazz, String order) throws Exception {
            String[] split = order.split("\\.");

            List<Field> fields = new ArrayList<Field>();
            for (String s : split) {
                Field field = clazz.getDeclaredField(s);
                fields.add(field);

                clazz = field.getType();
            }

            return fields;
        }

        private Object getValue(Object obj, List<Field> fields) throws Exception {
            Field typeField = fields.get(fields.size() - 1);
            Class type = typeField.getType();

            for (Field f : fields) {
                if (obj == null) {
                    return "";
                }

                Class<? extends Object> clazz = obj.getClass();

                if (Collection.class.isAssignableFrom(f.getType())) {
                    return "";
                }

                f.setAccessible(true);
                obj = f.get(obj);
            }

            if (obj == null) {
                return "";
            }

            return obj;
        }

    }

    public class ClassA {
        private String name;
        private ClassB b;

        @Override
        public String toString() {
            return name;
        }
    }

    public class ClassB {
        private String name;
        private ClassC c;
    }

    public class ClassC {
        private String name;
    }
}
