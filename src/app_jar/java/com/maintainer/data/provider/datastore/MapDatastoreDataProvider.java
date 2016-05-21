package com.maintainer.data.provider.datastore;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
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
import com.maintainer.data.model.MyClass;
import com.maintainer.data.model.MyField;
import com.maintainer.data.provider.DataProvider;
import com.maintainer.data.provider.DataProviderFactory;
import com.maintainer.util.Utils;

public class MapDatastoreDataProvider<T extends MapEntityImpl> extends DatastoreDataProvider<T> {
    private static final Logger log = Logger.getLogger(MapDatastoreDataProvider.class.getName());

    @Override
    @SuppressWarnings("unchecked")
    public T fromEntity(final Class<?> kind, final Entity entity, final int depth, final int currentDepth, final Map<com.maintainer.data.provider.Key, Object> cache) throws Exception {
        final T obj = super.fromEntity(kind, entity, depth, currentDepth, cache);

        boolean keysOnly = false;
        if (depth == currentDepth) {
            keysOnly = true;
        }

        try {
            final Map<String, Object> properties = entity.getProperties();
            for (final Entry<String, Object> e : properties.entrySet()) {
                final String field = e.getKey();

                switch (field) {
                case "id":
                case "identity":
                case "properties":
                    continue;
                }

                Object value = e.getValue();

                if (value != null) {
                    if (Key.class.isAssignableFrom(value.getClass())) {
                        final Key k = (Key) value;
                        com.maintainer.data.provider.Key nobodyelsesKey = createNobodyelsesKey(k);
                        Object cachedDepth = cache.get(nobodyelsesKey);
                        int d = Autocreate.MAX_DEPTH;

                        if (cachedDepth != null) {
                            d = (int) cachedDepth;
                        }

                        if (keysOnly || d < currentDepth) {
                            value = getKeyedOnly(nobodyelsesKey);
                        } else {
                            try {
                                value = get(nobodyelsesKey, depth, currentDepth + 1, cache);
                            } catch (CirularReferenceException e1) {
                                value = get(nobodyelsesKey, 1, 0, new HashMap<com.maintainer.data.provider.Key, Object>());
                            }
                        }
                    } else if (Text.class.isAssignableFrom(value.getClass())) {
                        value = ((Text) value).getValue();
                    } else if (Collection.class.isAssignableFrom(value.getClass())) {
                        final List<Object> list = new ArrayList<Object>((Collection<? extends Object>) value);

                        final ListIterator<Object> iterator = list.listIterator();
                        while(iterator.hasNext()) {
                            Object o = iterator.next();
                            if (Key.class.isAssignableFrom(o.getClass())) {
                                final Key k = (Key) o;
                                com.maintainer.data.provider.Key nobodyelsesKey = createNobodyelsesKey(k);

                                Object cachedDepth = cache.get(nobodyelsesKey);
                                int d = Autocreate.MAX_DEPTH;

                                if (cachedDepth != null) {
                                    d = (int) cachedDepth;
                                }

                                if (keysOnly ||  d < currentDepth) {
                                    o = getKeyedOnly(nobodyelsesKey);
                                } else {
                                    try {
                                        o = get(nobodyelsesKey, depth, currentDepth + 1, cache);
                                    } catch (CirularReferenceException e1) {
                                        o = get(nobodyelsesKey, 1, 0, new HashMap<com.maintainer.data.provider.Key, Object>());
                                    }
                                }
                                iterator.set(o);
                            }
                        }
                        value = list;
                    }
                    obj.put(field, value);
                }
            }
        } catch (final CirularReferenceException e) {

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return obj;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    protected Entity toEntity(Entity entity, final T target) throws Exception {

        entity = super.toEntity(entity, target);

        List<MyField> fields = Utils.getFields(target);

        for (MyField field : fields) {

            String fieldName = field.getName();
            switch (fieldName) {
            case "id":
            case "identity":
            case "properties":
                continue;
            }

            Object value = target.get(fieldName);

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
                entity.setProperty(fieldName, value);
            } else {
                entity.setUnindexedProperty(fieldName, value);
            }
        }

        return entity;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void autocreateFromField(final EntityBase target, final T existing, final MyField f) {
        f.setAccessible(true);
        if (f.hasAutocreate() && !f.embedded()) {
            try {
                MapEntityImpl mapEntityImpl = (MapEntityImpl) target;
                String fieldName = f.getName();
                Object value = mapEntityImpl.get(fieldName);
                if (value != null && MapEntityImpl.class.isAssignableFrom(value.getClass())) {
                    MapEntityImpl map = (MapEntityImpl) value;
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
                        if (MapEntityImpl.class.isAssignableFrom(value.getClass())) {
                            final MapEntityImpl mapEntityImpl2 = (MapEntityImpl) value;
                            mapEntityImpl2.setMyClass(f.getMyClass());
                        }
                        mapEntityImpl.set(fieldName, createOrUpdate(entity, f.readonly(), f.create(), f.update()));
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
                                if (MapEntityImpl.class.isAssignableFrom(o.getClass())) {
                                    final MapEntityImpl mapEntityImpl2 = (MapEntityImpl) o;
                                    mapEntityImpl2.setMyClass(f.getMyClass());
                                }
                                iterator.set(createOrUpdate(entity, f.readonly(), f.create(), f.update()));
                            }
                        }

                        if (removeThese != null && !removeThese.isEmpty()) {
                            removeThese.removeAll(list);
                            for (final Object object : removeThese) {
                                delete(object, f.embedded(), f.readonly(), f.delete());
                            }
                        }
                    }
                } else {
                    if (existing != null) {
                        final Object object = f.get(existing);
                        delete(object, f.embedded(), f.readonly(), f.delete());
                    }
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public T fromJson(Class<?> kind, String json) throws Exception {
        T obj = super.fromJson(kind, json);
        Gson gson = Utils.getGson();

        Map<String, Object> map = gson.fromJson(json, Utils.getItemType());

        List<MyField> fields = Utils.getFields(obj);
        obj = fromFields(obj, fields, map);
        return obj;
    }

    @SuppressWarnings({"unchecked"})
    public T fromFields(final T obj, final List<MyField> fields, Map<String, Object> map) throws Exception {
        Gson gson = Utils.getGson();

        for (MyField field : fields) {
            if ("properties".equals(field.getName())) {
                continue;
            }

            Class<?> type = field.getType();
            Object value = map.get(field.getName());

            if (value != null) {
                Class<?> valueType = value.getClass();

                if (Collection.class.isAssignableFrom(valueType)) {
                    String className = field.getMyClass().getName();
                    List<MyField> fields2 = Utils.getFields(className);

                    List<Object> list = new ArrayList<Object>((Collection) value);
                    List<Object> list2 = new ArrayList<Object>();

                    for (Object o : list) {
                        if (EntityBase.class.isAssignableFrom(type)) {
                            String json = gson.toJson(o);
                            T obj2 = super.fromJson(MapEntityImpl.class, json);
                            Map<String, Object> map2 = gson.fromJson(json, Utils.getItemType());
                            obj2 = fromFields(obj2, fields2, map2);
                            list2.add(obj2);
                        } else {
                            list2.add(o);
                        }
                    }

                    value = list2;
                } else if (MapEntityImpl.class.isAssignableFrom(type)) {
                    String json = gson.toJson(value);
                    T obj2 = super.fromJson(MapEntityImpl.class, json);

                    String className = field.getMyClass().getName();
                    List<MyField> fields2 = Utils.getFields(className);
                    value = fromFields(obj2, fields2, (Map<String, Object>) value);
                }
            }

            if (value == null) {
                Utils.setFieldValue(obj, field, null);
            } else {
                if (MapEntityImpl.class.isAssignableFrom(type)) {
                    Utils.setFieldValue(obj, field, value);
                } else if (EntityBase.class.isAssignableFrom(type)) {
                    String json2 = gson.toJson(value);
                    DataProvider<?> dataProvider = DataProviderFactory.instance().getDataProvider(type);
                    value = dataProvider.fromJson(type, json2);
                    Utils.setFieldValue(obj, field, value);
                } else if (Date.class.isAssignableFrom(type)) {
                    if (String.class == value.getClass()) {
                        Utils.setFieldValue(obj, field, Utils.convertToDate(value.toString()));
                    } else {
                        Utils.setFieldValue(obj, field, new Date(new BigDecimal(value.toString()).longValue()));
                    }
                } else {
                    Utils.setFieldValue(obj, field, Utils.convert(value, type));
                }
            }
        }

        return obj;
    }

    @Override
    public Class<?> getClazz(final Key k) throws ClassNotFoundException {
        Class<?> class1 = super.getClazz(k);
        if (class1 == null) {
            // String kind = k.getKind();
            // DataProvider<MyClass> dataProvider = (DataProvider<MyClass>) DataProviderFactory.instance().getDataProvider(MyClass.class);
            // com.maintainer.data.provider.Key key = com.maintainer.data.provider.Key.create(MyClass.class, kind, null);
            // MyClass clazz = dataProvider.get(key);
            // if (clazz != null) {
                class1 = MapEntityImpl.class;
            // }
        }
        return class1;
    }

    @Override
    protected String getKindName(T target) throws Exception {
        if (MapEntityImpl.class.isAssignableFrom(target.getClass())) {
            MapEntityImpl mapEntityImpl = target;
            MyClass myClass = mapEntityImpl.getMyClass();
            if (myClass != null) {
                return myClass.getName();
            }
        }
        return super.getKindName(target);
    }
}
