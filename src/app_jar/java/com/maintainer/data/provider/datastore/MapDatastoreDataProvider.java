package com.maintainer.data.provider.datastore;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Text;
import com.google.gson.Gson;
import com.maintainer.data.model.Autocreate;
import com.maintainer.data.model.EntityBase;
import com.maintainer.data.model.MapEntityImpl;
import com.maintainer.util.MyField;
import com.maintainer.util.Utils;

public class MapDatastoreDataProvider<T extends MapEntityImpl> extends DatastoreDataProvider<T> {
    private static final Logger log = Logger.getLogger(MapDatastoreDataProvider.class.getName());

    @Override
    @SuppressWarnings("unchecked")
    public T fromEntity(final Class<?> kind, final Entity entity) throws Exception {
        final T obj = super.fromEntity(kind, entity);

        try {
            final Map<String, Object> properties = entity.getProperties();
            for (final Entry<String, Object> e : properties.entrySet()) {
                final String field = e.getKey();

                switch (field) {
                case "id":
                case "created":
                case "modified":
                case "identity":
                case "properties":
                    continue;
                }

                Object value = e.getValue();

                if (value != null) {
                    if (Key.class.isAssignableFrom(value.getClass())) {
                        final Key k = (Key) value;
                        final String className = k.getKind();
                        final Class<?> class1 = Class.forName(className);
                        value = get(com.maintainer.data.provider.Key.create(class1, k.getId()));
                    } else if (Text.class.isAssignableFrom(value.getClass())) {
                        value = ((Text) value).getValue();
                    } else if (Collection.class.isAssignableFrom(value.getClass())) {
                        final List<Object> list = new ArrayList<Object>((Collection<? extends Object>) value);

                        final ListIterator<Object> iterator = list.listIterator();
                        while(iterator.hasNext()) {
                            Object o = iterator.next();
                            if (Key.class.isAssignableFrom(o.getClass())) {
                                final Key k = (Key) o;
                                final String className = k.getKind();
                                final Class<?> class1 = Class.forName(className);
                                o = get(com.maintainer.data.provider.Key.create(class1, k.getId()));
                                iterator.set(o);
                            }
                        }
                        value = list;
                    }
                    obj.put(field, value);
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return obj;
    }

    @Override
    protected Entity toEntity(Entity entity, final T target) throws Exception {

        entity = super.toEntity(entity, target);

        for (final Entry<String, Object> entry : target.entrySet()) {
            final String field = entry.getKey();

            switch (field) {
            case "id":
            case "created":
            case "modified":
            case "identity":
            case "properties":
                continue;
            }

            Object value = entry.getValue();

            try {
                if (value != null) {
                    if (EntityBase.class.isAssignableFrom(value.getClass())) {
                        final EntityBase base = (EntityBase) value;
                        value = createDatastoreKey(base.getKey());
                    } else if (Collection.class.isAssignableFrom(value.getClass())) {
                        final List<Object> list = new ArrayList<Object>((Collection<Object>) value);
                        value = list;

                        final ListIterator<Object> iterator = list.listIterator();
                        while(iterator.hasNext()) {
                            final Object o = iterator.next();
                            if (EntityBase.class.isAssignableFrom(o.getClass())) {
                                final EntityBase base = (EntityBase) o;
                                final Key key = createDatastoreKey(base.getKey());
                                iterator.set(key);
                            }
                        }
                    }
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }

            if (value != null && String.class.isAssignableFrom(value.getClass())) {
                final String string = (String) value;
                if (string.length() > 500) {
                    value = new Text(string);
                }
            }

            final boolean indexed = target.isIndexed(field);
            if (indexed) {
                entity.setProperty(field, value);
            } else {
                entity.setUnindexedProperty(field, value);
            }
        }

        return entity;
    }

    @Override
    protected void autocreateFromField(final EntityBase target, final T existing, final MyField f) {
        f.setAccessible(true);
        final Autocreate autocreate = f.getAnnotation(Autocreate.class);
        if (autocreate != null && !autocreate.embedded()) {
            try {
                MapEntityImpl mapEntityImpl = (MapEntityImpl) target;
                String fieldName = f.getName();
                Object value = mapEntityImpl.get(fieldName);
                if (value != null && Map.class.isAssignableFrom(value.getClass())) {
                    Map map = (Map) value;
                    String keyString = (String) map.get("id");
                    if (keyString != null) {
                        com.maintainer.data.provider.Key key = null;
                        try {
                            key = com.maintainer.data.provider.Key.fromString(keyString);
                        } catch (Exception e) {
                            // ignored
                        }

                        if (key != null) {
                            Class<?> kind = key.getKind();
                            if (EntityBase.class.isAssignableFrom(kind)) {
                                Gson gson = Utils.getGson();
                                String json = gson.toJson(value);
                                value = gson.fromJson(json, kind);
                            }
                        }
                    }
                }

                if (value != null) {
                    if (EntityBase.class.isAssignableFrom(value.getClass())) {
                        final EntityBase entity = (EntityBase) value;
                        mapEntityImpl.set(fieldName, createOrUpdate(entity, autocreate));
                    } else if (Collection.class.isAssignableFrom(value.getClass())) {
                        final List<Object> list = new ArrayList<Object>();
                        if (value != null) {
                            list.addAll((Collection<Object>) value);
                        }

                        List<Object> removeThese = null;
                        if (existing != null) {
                            Collection<Object> collection = (Collection<Object>) f.get(existing);
                            if (collection != null) {
                                removeThese = new ArrayList<Object>(collection);
                            }
                        }

                        final ListIterator<Object> iterator = list.listIterator();
                        while(iterator.hasNext()) {
                            final Object o = iterator.next();
                            if (o == null) {
                                continue;
                            }
                            if (EntityBase.class.isAssignableFrom(o.getClass())) {
                                final EntityBase entity = (EntityBase) o;
                                iterator.set(createOrUpdate(entity, autocreate));
                            }
                        }

                        if (removeThese != null && !removeThese.isEmpty()) {
                            removeThese.removeAll(list);
                            for (final Object object : removeThese) {
                                delete(object, autocreate);
                            }
                        }
                    }
                } else {
                    if (existing != null) {
                        final Object object = f.get(existing);
                        delete(object, autocreate);
                    }
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected ArrayList<MyField> getFields(final Object target, boolean isRecurse) {
        final Map<String, MyField> fieldMap = new LinkedHashMap<String, MyField>();
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            final Field[] fields2 = clazz.getDeclaredFields();
            for (int i = 0; i < fields2.length; i++) {
                final Field f = fields2[i];
                final String name = f.getName();

                final MyField myField = new MyField(f);
                if (!fieldMap.containsKey(name)) {
                    fieldMap.put(name, myField);
                }
            }

            if (!isRecurse) {
                break;
            }

            clazz = clazz.getSuperclass();
        }
        return new ArrayList<MyField>(fieldMap.values());
    }
}
