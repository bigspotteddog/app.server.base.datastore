package com.maintainer.data.provider.datastore;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.commons.lang3.StringUtils;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.Query.SortPredicate;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.common.base.Strings;
import com.maintainer.data.model.Autocreate;
import com.maintainer.data.model.EntityBase;
import com.maintainer.data.model.EntityImpl;
import com.maintainer.data.model.MapEntityImpl;
import com.maintainer.data.model.MyField;
import com.maintainer.data.model.NotIndexed;
import com.maintainer.data.model.NotStored;
import com.maintainer.data.provider.DataProvider;
import com.maintainer.data.provider.DataProviderFactory;
import com.maintainer.data.provider.Filter;
import com.maintainer.data.provider.Query;
import com.maintainer.util.FieldSortComparator;
import com.maintainer.util.JsonString;
import com.maintainer.util.Utils;

public class DatastoreDataProvider<T extends EntityBase> extends AbstractDatastoreDataProvider<T> {
    private static final Logger log = Logger.getLogger(DatastoreDataProvider.class.getName());
//    private static final Cache<String, Object> cache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build();
    private static final MemcacheService memcache = MyMemcacheServiceFactory.getMemcacheService();

    private boolean nocache;
    private boolean local;
    private DatastoreService datastore;

    public DatastoreDataProvider() {}

    public DatastoreDataProvider(final boolean nocache) {
        this.nocache = nocache;
        this.local = nocache;
    }

    @Override
    public Object getId(final Object object) {
        if (object != null && Key.class.isAssignableFrom(object.getClass())) {
            final Key key = (Key) object;
            return key.getId();
        }
        return super.getId(object);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(final com.maintainer.data.provider.Key key) throws Exception {
        ThreadLocalCache threadLocalCache = ThreadLocalCache.get();
        Object obj = threadLocalCache.get(key);
        if (obj == null) {
            threadLocalCache.put(key, new Integer(1));
        } else {
            if (Integer.class.equals(obj.getClass())) {
                log.severe("Circular reference detected for key: " + key.asString());
                if (key.getKind() == null) {
                    T keyedOnly = (T) getKeyedOnly(MapEntityImpl.class, key);
                    return keyedOnly;
                } else {
                    T keyedOnly = (T) getKeyedOnly(key);
                    return keyedOnly;
                }
            }
        }

        final T cached = (T) getCached(key);
        if (cached != null) {
            if (cached.getKey() == null) {
                cached.setKey(key);
            }
            // log.fine(key + " returned from cache.");
            threadLocalCache.put(key, cached);
            return cached;
        }

        final Key k = createDatastoreKey(key);
        try {
            final Entity entity = getEntity(k);
            Class<?> kind = getClazz(k);
            final T fetched = getTarget(kind, entity);
            putCache(key, fetched);

            threadLocalCache.put(key, fetched);

            return fetched;
        } catch (final EntityNotFoundException e) {
            //ignore, it will just be null
        }

        threadLocalCache.remove(key);
        return null;
    }

    @Override
    public List<T> getAll(final Class<?> kind) throws Exception {
        final String kindName = Utils.getKindName(kind);

        final com.google.appengine.api.datastore.Query q = new com.google.appengine.api.datastore.Query(kindName);
        final FetchOptions options = FetchOptions.Builder.withDefaults();

        final List<T> list = getEntities(q, options, 0);
        return list;
    }

    @Override
    public T post(final T target) throws Exception {
        autocreate(target);

        MyField created = Utils.getField(target, "created");
        if (created == null) {
            created = new MyField("created", Date.class);
        }
        Utils.setFieldValue(target, created, new Date());

        final Entity entity = toEntity(null, target);

        final DatastoreService datastore = getDatastore();
        final Key posted = datastore.put(entity);

        final com.maintainer.data.provider.Key nobodyelsesKey = createNobodyelsesKey(posted);
        target.setKey(nobodyelsesKey);
        target.setId(getEncodedKeyString(nobodyelsesKey));

        invalidateCached(nobodyelsesKey);

        ThreadLocalCache.get().put(nobodyelsesKey, target);

        return target;
    }

    @Override
    public T put(T target) throws Exception {
        final com.maintainer.data.provider.Key nobodyelsesKey = target.getKey();
        if (nobodyelsesKey != null) {
            final T existing = get(nobodyelsesKey);

            if (checkEqual(target, existing)) {
                ThreadLocalCache.get().put(nobodyelsesKey, target);
                return target;
            } else {
                // log.fine(nobodyelsesKey + " changed.");
            }
        }

        if (target.getId() == null) {
            target = post(target);
            return target;
        }

        autocreate(target);

        MyField modified = Utils.getField(target, "modified");
        if (modified == null) {
            modified = new MyField("modified", Date.class);
        }
        Utils.setFieldValue(target, modified, new Date());

        Entity entity = getEntity(createDatastoreKey(nobodyelsesKey));
        entity = toEntity(entity, target);

        final DatastoreService datastore = getDatastore();
        datastore.put(entity);

        invalidateCached(nobodyelsesKey);

        ThreadLocalCache.get().put(nobodyelsesKey, target);

        return target;
    }

    @Override
    public com.maintainer.data.provider.Key delete(final com.maintainer.data.provider.Key key) throws Exception {
        autodelete(key);

        final DatastoreService datastore = getDatastore();
        datastore.delete(createDatastoreKey(key));
        invalidateCached(key);
        return key;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> find(final Query query) throws Exception {
        final DatastoreService datastore = getDatastore();

        Class<?> kind = query.getKind();
        if (query.getKey() != null) {
            try {
                final Key key = createDatastoreKey(query.getKey());
                final Entity entity = datastore.get(key);

                final T target = getTarget(kind, entity);

                final ResultListImpl<T> list = new ResultListImpl<T>();
                list.add(target);
                putCache(target.getKey(), target);
                return list;
            } catch (final EntityNotFoundException e) {
                e.printStackTrace();
            }
            return ResultListImpl.emptyList();
        }

        final String pageDirection = query.getPageDirection();

        final com.google.appengine.api.datastore.Query q = getQuery(query);

        if (query.getParent() != null) {
            q.setAncestor(createDatastoreKey(query.getParent()));
        }

        FetchOptions options = FetchOptions.Builder.withDefaults();

        try {
            if (Query.PREVIOUS.equals(pageDirection)) {
                final Cursor fromWebSafeString = Cursor.fromWebSafeString(query.getPreviousCursor());
                options.startCursor(fromWebSafeString);
            } else if (Query.NEXT.equals(pageDirection)) {
                final Cursor fromWebSafeString = Cursor.fromWebSafeString(query.getNextCursor());
                options.startCursor(fromWebSafeString);
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }

        if (query.getOffset() > 0) {
            options.offset(query.getOffset());
        }

        final int limit = query.getLimit();
        if (limit > 0) {
            options.limit(limit + 1);
        }

        if (query.isKeysOnly()) {
            q.setKeysOnly();
        }

        final ResultListImpl<T> list = getEntities(q, options, limit);

        if (!list.isEmpty()) {
            String order = query.getOrder();
            if (!Utils.isEmpty(order)) {
                Collections.sort(list, new FieldSortComparator(kind, order));
            }

            boolean hasMoreRecords = false;
            if (limit > 0) {
                hasMoreRecords = list.size() > limit;
            }

            if (hasMoreRecords) {
                list.remove(list.size() - 1);
            }

            if (Query.PREVIOUS.equals(pageDirection)) {
                Collections.reverse(list);
                final Cursor end = list.getStartCursor();
                final Cursor start = list.getEndCursor();
                list.setStartCursor(start);
                list.setEndCursor(end);

                if (!hasMoreRecords) {
                    list.setStartCursor(null);
                }
            } else if (Query.NEXT.equals(pageDirection)) {
                if (!hasMoreRecords) {
                    list.setEndCursor(null);
                }
            } else {
                if (containsEqualOrIn(q)) {
                    list.setStartCursor(null);
                    list.setEndCursor(null);
                } else {
                    if (!hasMoreRecords) {
                        list.setEndCursor(null);
                    }

                    options = cloneOptionsWithoutCursors(options);

                    final boolean empty = testBoundary(q, options);
                    if (empty) {
                        list.setStartCursor(null);
                    }
                }
            }

            if (options.getStartCursor() == null) {
                list.setStartCursor(null);
            }
        }

        return list;
    }

    protected boolean checkEqual(final T target, final T existing) throws Exception {
        return isEqual(target, existing);
    }

    protected Entity getEntity(final Key key) throws EntityNotFoundException {
        final DatastoreService datastore = getDatastore();
        final Entity entity = datastore.get(key);
        return entity;
    }

    @SuppressWarnings( {"unchecked", "rawtypes"} )
    public T fromEntity(final Class<?> kind, final Entity entity) throws Exception {
        T obj = null;

        try {
            if (kind != null) {
                final Constructor<T> c = (Constructor<T>) kind.getDeclaredConstructor((Class[]) null);
                c.setAccessible(true);
                obj = c.newInstance((Object[]) null);
            } else {
                obj = (T) new MapEntityImpl();
            }

            final Map<String, Object> properties = entity.getProperties();
            final List<MyField> fields = Utils.getFields(obj);
            for (final MyField f : fields) {
                final NotStored notStored = f.getNotStored();
                if (notStored != null) {
                    continue;
                }

                f.setAccessible(true);

                final Autocreate autocreate = f.getAutocreate();
                final String key = f.getName();
                Object value = properties.get(key);

                if (value != null) {
                    if (autocreate != null && autocreate.embedded()) {
                        value = getEmbedded(f, value);
                    } else if (Key.class.isAssignableFrom(value.getClass())) {
                        final Key k = (Key) value;
                        final com.maintainer.data.provider.Key key2 = createNobodyelsesKey(k);
                        if (autocreate != null && autocreate.keysOnly()) {
                            value = getKeyedOnly(key2);
                        } else {
                            value = get(key2);
                        }
                    } else if (Blob.class.isAssignableFrom(value.getClass())) {
                        value = ((Blob) value).getBytes();
                    } else if (Text.class.isAssignableFrom(value.getClass())) {
                        value = ((Text) value).getValue();
                    } else if (Double.class.isAssignableFrom(value.getClass()) && BigDecimal.class.isAssignableFrom(f.getType())) {
                        value = new BigDecimal(value.toString());
                    } else if (List.class.isAssignableFrom(value.getClass())) {
                        final List<Object> list = new ArrayList<Object>((Collection<? extends Object>) value);

                        final ListIterator<Object> iterator = list.listIterator();
                        while(iterator.hasNext()) {
                            Object o = iterator.next();
                            if (Key.class.isAssignableFrom(o.getClass())) {
                                final Key k = (Key) o;
                                final com.maintainer.data.provider.Key key2 = createNobodyelsesKey(k);
                                if (autocreate != null && autocreate.keysOnly()) {
                                    o = getKeyedOnly(key2);
                                } else {
                                    o = get(key2);
                                }
                                iterator.set(o);
                            }
                        }

                        if (LinkedHashSet.class.isAssignableFrom(f.getType())) {
                            value = new LinkedHashSet(list);
                        } else if (Set.class.isAssignableFrom(f.getType()) || HashSet.class.isAssignableFrom(f.getType())) {
                            value = new HashSet(list);
                        } else {
                            value = list;
                        }
                    } else if (Set.class.isAssignableFrom(value.getClass())) {
                        final List<Object> list = new ArrayList<Object>((Collection<? extends Object>) value);

                        final ListIterator<Object> iterator = list.listIterator();
                        while(iterator.hasNext()) {
                            Object o = iterator.next();
                            if (Key.class.isAssignableFrom(o.getClass())) {
                                final Key k = (Key) o;
                                final com.maintainer.data.provider.Key key2 = createNobodyelsesKey(k);
                                if (autocreate != null && autocreate.keysOnly()) {
                                    o = getKeyedOnly(key2);
                                } else {
                                    o = get(key2);
                                }
                                iterator.set(o);
                            }
                        }

                        if (LinkedHashSet.class.isAssignableFrom(f.getType())) {
                            value = new LinkedHashSet(list);
                        } else if (Set.class.isAssignableFrom(f.getType()) || HashSet.class.isAssignableFrom(f.getType())) {
                            value = new HashSet(list);
                        } else {
                            value = list;
                        }
                    }

                    if (JsonString.class.isAssignableFrom(f.getType())) {
                        value = new JsonString(value.toString());
                    }

                    Utils.setFieldValue(obj, f, value);
                }
            }

            final Key key = entity.getKey();
            final com.maintainer.data.provider.Key nobodyelsesKey = createNobodyelsesKey(key);

            obj.setKey(nobodyelsesKey);
            obj.setIdentity(nobodyelsesKey.getId());
            obj.setId(getEncodedKeyString(nobodyelsesKey));

            final Autocreate autocreate = obj.getClass().getAnnotation(Autocreate.class);
            if (autocreate != null) {
                if (!Autocreate.EMPTY.equals(autocreate.parent())) {
                    if (key.getParent() != null) {
                        final MyField field = Utils.getField(obj, autocreate.parent());
                        field.setAccessible(true);

                        EntityImpl parent = null;
                        final Autocreate fieldAutocreate = field.getAutocreate();
                        if (fieldAutocreate != null && fieldAutocreate.keysOnly()) {
                            parent = getKeyedOnly(nobodyelsesKey.getParent());
                        } else {
                            parent = (EntityImpl) get(nobodyelsesKey.getParent());
                        }
                        field.set(obj, parent);
                        obj.setParent(parent);
                    }
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return obj;
    }

    public EntityImpl getKeyedOnly(final Class<?> class1, final com.maintainer.data.provider.Key key) throws Exception {
        return Utils.getKeyedOnly(class1, key);
    }

    public EntityImpl getKeyedOnly(final com.maintainer.data.provider.Key key) throws Exception {
        return Utils.getKeyedOnly(key);
    }

    protected Object getEmbedded(final MyField f, Object value) throws Exception {
        if (Text.class.isAssignableFrom(value.getClass())) {
            value = ((Text) value).getValue();
        }
        final String json = (String) value;
        value = Utils.getGson().fromJson(json, f.getGenericType());
        return value;
    }

    protected DatastoreService getDatastore() {
        if (datastore == null) {
            datastore = DatastoreServiceFactory.getDatastoreService();
        }
        return datastore;
    }

    private Entity newEntity(final EntityBase parent, final String kind) {
        if (parent != null) {
            return new Entity(kind, createDatastoreKey(parent.getKey()));
        }
        return new Entity(kind);
    }

    @SuppressWarnings("unchecked")
    protected Entity toEntity(Entity entity, final T target) throws Exception {

        final Class<? extends EntityBase> clazz = target.getClass();

        if (entity == null) {
            final Autocreate annotation = getClassAutocreate(clazz);

            EntityBase parent = target.getParent();
            if (annotation != null && !Autocreate.EMPTY.equals(annotation.parent())) {
                parent = (EntityBase) Utils.getFieldValue(target, annotation.parent());
            }

            final String kindName = getKindName(target);

            if (annotation != null && !Autocreate.EMPTY.equals(annotation.id())) {

                Object id = Utils.getFieldValue(target, annotation.id());

                if (id != null) {
                    if (EntityBase.class.isAssignableFrom(id.getClass())) {
                        id = ((EntityBase) id).getKey().asString();
                    }

                    Key key = null;
                    key = createDatastoreKey(parent, kindName, id);
                    final com.maintainer.data.provider.Key nobodyelsesKey = createNobodyelsesKey(key);
                    target.setId(getEncodedKeyString(nobodyelsesKey));
                    target.setIdentity(id);
                    entity = newEntity(key);
                    entity.setUnindexedProperty("identity", id);
                } else {
                    entity = newEntity(parent, kindName);
                }
            } else {
                entity = newEntity(parent, kindName);
            }
        }

        final List<MyField> fields = Utils.getFields(target);

        for (final MyField f : fields) {
            final NotStored notStored = f.getNotStored();
            if (notStored != null) {
                continue;
            }

            f.setAccessible(true);
            Object value = f.get(target);

            final Autocreate autocreate = f.getAutocreate();

            if (autocreate != null) {
                if (autocreate.readonly()) {
                    if (entity.getProperty(f.getName()) != null) {
                        continue;
                    }
                }

                try {
                    if (value != null) {
                        if (autocreate.embedded()) {
                            value = Utils.getGson().toJson(value);
                        } else if (EntityBase.class.isAssignableFrom(value.getClass())) {
                            final EntityBase base = (EntityBase) value;
                            final com.maintainer.data.provider.Key key = base.getKey();
                            if (key != null) {
                                value = createDatastoreKey(key);
                            } else {
                                value = null;
                            }
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
            }

            if (value != null) {
                if (JsonString.class.isAssignableFrom(value.getClass())) {
                    value = ((JsonString) value).getString();
                }

                if (String.class.isAssignableFrom(value.getClass())) {
                    final String string = (String) value;
                    if (string.length() > 500) {
                        value = new Text(string);
                    }
                } else if (BigDecimal.class.isAssignableFrom(value.getClass())) {
                    final BigDecimal decimal = (BigDecimal) value;
                    value = decimal.doubleValue();
                } else if (value.getClass().isAssignableFrom(byte[].class)) {
                    value = new Blob((byte[]) value);
                }
            }

            final NotIndexed notIndexed = f.getNotIndexed();
            if (notIndexed == null) {
                entity.setProperty(f.getName(), value);
            } else {
                entity.setUnindexedProperty(f.getName(), value);
            }
        }

        return entity;
    }

    protected String getKindName(T target) throws Exception {
        Class<?> clazz = target.getClass();
        String kindName = Utils.getKindName(clazz);
        return kindName;
    }

    @SuppressWarnings("unchecked")
    private Autocreate getClassAutocreate(final Class<? extends EntityBase> clazz) {
        Class<? extends EntityBase> class1 = clazz;

        while(class1 != null) {
            Autocreate annotation = class1.getAnnotation(Autocreate.class);
            if (annotation != null) {
                return annotation;
            }
            class1 = (Class<? extends EntityBase>) class1.getSuperclass();
        }
        return null;
    }

    private Entity newEntity(final Key key) {
        return new Entity(key);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object convertValue(Object value) {
        if (EntityImpl.class.isAssignableFrom(value.getClass())) {
            final EntityImpl entity = (EntityImpl) value;
            value = createDatastoreKey(entity.getKey());
        } else if (com.maintainer.data.provider.Key.class.isAssignableFrom(value.getClass())) {
            final com.maintainer.data.provider.Key k = (com.maintainer.data.provider.Key) value;
            value = createDatastoreKey(k);
        } else if (Collection.class.isAssignableFrom(value.getClass())) {
            List list = new ArrayList();
            Collection c = (Collection) value;
            Iterator i = c.iterator();
            while (i.hasNext()) {
                Object v = i.next();
                v = convertValue(v);
                list.add(v);
            }
            value = list;
        }

        return value;
    }

    private void addFilter(final com.google.appengine.api.datastore.Query q, final String propertyName, final FilterOperator operator, Object value) {
        value = convertValue(value);
        q.addFilter(propertyName.trim(), operator, value);
    }

    private boolean containsEqualOrIn(final com.google.appengine.api.datastore.Query q) {
        for (final FilterPredicate f : q.getFilterPredicates()) {
            final FilterOperator operator = f.getOperator();
            if (FilterOperator.IN == operator || FilterOperator.EQUAL == operator) {
                return true;
            }
        }
        return false;
    }

    private com.google.appengine.api.datastore.Query getQuery(final Query query) throws Exception {
        String kindName = null;
        Class<?> clazz = query.getKind();

        if (clazz != null) {
            kindName = Utils.getKindName(clazz);
        } else {
            kindName = query.getKindName();
        }

        final com.google.appengine.api.datastore.Query q = new com.google.appengine.api.datastore.Query(kindName);

        for (final Filter e : query.getFilters()) {
            final String condition = e.getCondition();

            final String key = getFieldFromCondition(condition);
            final String op = getOperatorFromCondition(condition);

            final FilterOperator operator = getOperator(op);

            Class<?> keyType = null;
            if (query.getKind() != null && !MapEntityImpl.class.equals(query.getKind().getClass())) {
                keyType = Utils.getKeyType(query.getKind(), key);
            } else {
                keyType = Utils.getKeyType(kindName, key);
            }

            Object value = e.getValue();
            value = Utils.convert(value, keyType);

            addFilter(q, key, operator, value);
        }

        final String pageDirection = query.getPageDirection();

        if (!Strings.isNullOrEmpty(query.getOrder())) {
            final String[] fields = StringUtils.split(query.getOrder(), ',');
            for (String field : fields) {
                if (field.startsWith("-")) {
                    SortDirection sortDirection = SortDirection.DESCENDING;
                    if (Query.PREVIOUS.equals(pageDirection)) {
                        sortDirection = SortDirection.ASCENDING;
                    }

                    field = field.substring(1);
                    if (!"id".equals(field.toLowerCase())) {
                        addSortField(q, field, sortDirection);
                    }
                } else {
                    if (!"id".equals(field.toLowerCase())) {
                        SortDirection sortDirection = SortDirection.ASCENDING;
                        if (Query.PREVIOUS.equals(pageDirection)) {
                            sortDirection = SortDirection.DESCENDING;
                        }

                        if (field.startsWith("+")) {
                            field = field.substring(1);
                        }
                        addSortField(q, field, sortDirection);
                    }
                }
            }
        } else if (hasInequalityFilter(query)) {
            final String field = getInequalityField(query);
            q.addSort(field);
        }

        if (Query.PREVIOUS.equals(pageDirection)) {
            q.addSort(Entity.KEY_RESERVED_PROPERTY, SortDirection.DESCENDING);
        } else {
            q.addSort(Entity.KEY_RESERVED_PROPERTY, SortDirection.ASCENDING);
        }
        return q;
    }

    private void addSortField(final com.google.appengine.api.datastore.Query q, String field, SortDirection sortDirection) {
        if (field.indexOf('.') > -1) {
            return;
        }

        q.addSort(field, sortDirection);
    }

    private String getInequalityField(final Query query) {
        for (final Filter f : query.getFilters()) {
            final String condition = f.getCondition();

            final String op = this.getOperatorFromCondition(condition);
            final boolean b = this.isInequalityOperator(op);
            if (b) {
                return this.getFieldFromCondition(condition);
            }
        }
        return null;
    }

    private boolean hasInequalityFilter(final Query query) {
        for (final Filter f : query.getFilters()) {
            final String condition = f.getCondition();

            final String op = this.getOperatorFromCondition(condition);
            final boolean b = this.isInequalityOperator(op);
            if (b) {
                return true;
            }
        }

        return false;
    }

    private boolean testBoundary(com.google.appengine.api.datastore.Query q, final FetchOptions options) throws Exception {
        q = reverse(q);
        q.setKeysOnly();
        options.limit(1);

        final DatastoreService datastore = getDatastore();

        try {
            final PreparedQuery p = datastore.prepare(q);
            final List<Entity> list = p.asList(options);
            if (!list.isEmpty()) {
                return false;
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    private com.google.appengine.api.datastore.Query reverse(final com.google.appengine.api.datastore.Query q) {
        final com.google.appengine.api.datastore.Query q2 = new com.google.appengine.api.datastore.Query(q.getKind());

        for (final FilterPredicate f : q.getFilterPredicates()) {
            final FilterOperator operator = f.getOperator();
            if (FilterOperator.GREATER_THAN == operator) {
                q2.addFilter(f.getPropertyName(), FilterOperator.LESS_THAN_OR_EQUAL, f.getValue());
            } else if (FilterOperator.GREATER_THAN_OR_EQUAL == operator) {
                q2.addFilter(f.getPropertyName(), FilterOperator.LESS_THAN, f.getValue());
            } else if (FilterOperator.LESS_THAN == operator) {
                q2.addFilter(f.getPropertyName(), FilterOperator.GREATER_THAN_OR_EQUAL, f.getValue());
            } else if (FilterOperator.LESS_THAN_OR_EQUAL == operator) {
                q2.addFilter(f.getPropertyName(), FilterOperator.GREATER_THAN, f.getValue());
            } else {
                q2.addFilter(f.getPropertyName(), operator, f.getValue());
            }
        }

        for (final SortPredicate s : q.getSortPredicates()) {
            final SortPredicate reverse = s.reverse();
            q2.addSort(reverse.getPropertyName(), reverse.getDirection());
        }

        if (q.getAncestor() != null) {
            q2.setAncestor(q.getAncestor());
        }

        if (q.isKeysOnly()) {
            q2.setKeysOnly();
        }

        return q2;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> getAll(final Collection<com.maintainer.data.provider.Key> keysNeeded) throws Exception {
        final List<T> list = new ArrayList<T>(keysNeeded.size());

        final Map<com.maintainer.data.provider.Key, Object> map = getCachedAndTrimKeysNeeded(keysNeeded);
        for (final Entry<com.maintainer.data.provider.Key, Object> e : map.entrySet()) {
            final com.maintainer.data.provider.Key key = e.getKey();
            final T t = (T) e.getValue();

            if (t != null) {
                // because the keys are not in the cache
                if (t.getKey() ==  null) {
                    t.setKey(key);
                    t.setId(getEncodedKeyString(key));
                }

                list.add(t);
            }
        }

        final List<Key> keys = new ArrayList<Key>();
        for (final com.maintainer.data.provider.Key key : keysNeeded) {
            keys.add(createDatastoreKey(key));
        }

        final Map<com.maintainer.data.provider.Key, Object> needsToBeCachedMap = new LinkedHashMap<com.maintainer.data.provider.Key, Object>();
        final Map<Key, Entity> map3 = getDatastore().get(keys);
        for (final Entry<Key, Entity> e : map3.entrySet()) {
            final Key key = e.getKey();
            final Entity entity = e.getValue();

            Class<?> kind = getClazz(key);
            final T target = getTarget(kind, entity);

            list.add(target);
            needsToBeCachedMap.put(target.getKey(), target);
        }
        putAllCache(needsToBeCachedMap);

        return list;
    }

    @SuppressWarnings("unchecked")
    protected ResultListImpl<T> getEntities(final com.google.appengine.api.datastore.Query q, FetchOptions options, final int limit) throws Exception {
        final boolean isKeysOnly = setKeysOnly(q);

        final DatastoreService datastore = getDatastore();
        final PreparedQuery p = datastore.prepare(q);

        final List<Entity> entities = new ArrayList<Entity>();
        final List<com.maintainer.data.provider.Key> keysNeeded = new ArrayList<com.maintainer.data.provider.Key>();

        final Map<Key, String> cursors = new LinkedHashMap<Key, String>();
        Cursor start = null;
        Cursor end = null;
        boolean removedCursors = false;

        for (int i = 0; i < 2; i++) {
            final QueryResultIterator<Entity> iterator = p.asQueryResultIterator(options);

            try {
                while (iterator.hasNext()) {
                    final Entity e = iterator.next();
                    if (start == null) {
                        start = iterator.getCursor();
                    }
                    final Key k = e.getKey();
                    final com.maintainer.data.provider.Key key = createNobodyelsesKey(k);
                    keysNeeded.add(key);
                    entities.add(e);

                    if (limit > 0 && entities.size() >= limit && end == null) {
                        end = iterator.getCursor();
                    }
                }
                break;
            } catch (final IllegalArgumentException e) {
                if (options != null && (options.getStartCursor() != null || options.getEndCursor() != null)) {
                    System.out.println("Cursor may not be relevant for query. Trying again without cursors.");
                    removedCursors = true;
                    options = cloneOptionsWithoutCursors(options);
                } else {
                    e.printStackTrace();
                    break;
                }
            } catch (final Exception e) {
                e.printStackTrace();
                break;
            }
        }

        if (isKeysOnly) {
            final ResultListImpl<T> list = new ResultListImpl<T>(keysNeeded.size());
            for (final com.maintainer.data.provider.Key key : keysNeeded) {
                final EntityImpl keyedOnly = getKeyedOnly(key);
                list.add((T) keyedOnly);

                ThreadLocalCache.get().put(key, keyedOnly);
            }
            list.setStartCursor(start);
            list.setEndCursor(end);
            list.setRemovedCursors(removedCursors);

            return list;
        }

        final Map<com.maintainer.data.provider.Key, Object> map = new LinkedHashMap<com.maintainer.data.provider.Key, Object>();
        final Map<com.maintainer.data.provider.Key, Object> map2 = getCachedAndTrimKeysNeeded(keysNeeded);

        if (!map2.isEmpty()) {
            map.putAll(map2);
        }

        if (!keysNeeded.isEmpty()) {
            final List<Key> keys = new ArrayList<Key>();
            for (final com.maintainer.data.provider.Key key : keysNeeded) {
                keys.add(createDatastoreKey(key));
            }

            final Map<com.maintainer.data.provider.Key, Object> needsToBeCachedMap = new LinkedHashMap<com.maintainer.data.provider.Key, Object>();
            final Map<Key, Entity> map3 = datastore.get(keys);
            for (final Entry<Key, Entity> e : map3.entrySet()) {
                final Key key = e.getKey();
                final Entity entity = e.getValue();

                final String cursor = cursors.get(key);
                if (cursor != null) {
                    entity.setProperty("cursor", cursor);
                }

                Class<?> kind = getClazz(key);
                final T target = getTarget(kind, entity);

                map.put(target.getKey(), target);
                needsToBeCachedMap.put(target.getKey(), target);
            }
            putAllCache(needsToBeCachedMap);
        }

        final ResultListImpl<T> list = new ResultListImpl<T>(map.size());
        for (final Entity e : entities) {
            final Key k = e.getKey();
            final com.maintainer.data.provider.Key key = createNobodyelsesKey(k);
            final T o = (T) map.get(key);
            if (o != null) {
                o.setKey(key);
                list.add(o);

                ThreadLocalCache.get().put(key, o);
            }
        }

        list.setStartCursor(start);
        list.setEndCursor(end);
        list.setRemovedCursors(removedCursors);

        return list;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected T getTarget(Class<?> kind, final Entity entity) throws Exception {
        DataProvider<?> dataProvider = DataProviderFactory.instance().getDataProvider(kind);
        final T target = (T) ((DatastoreDataProvider) dataProvider).fromEntity(kind, entity);
        return target;
    }

    protected boolean setKeysOnly(final com.google.appengine.api.datastore.Query q) {
        final boolean isKeysOnly = q.isKeysOnly();

        if (!isKeysOnly) {
            q.setKeysOnly();
        }
        return isKeysOnly;
    }

    public Map<com.maintainer.data.provider.Key, Object> getCachedAndTrimKeysNeeded(final Collection<com.maintainer.data.provider.Key> keysNeeded) throws Exception {
        final Map<com.maintainer.data.provider.Key, Object> map2 = getAllCache(keysNeeded);
        keysNeeded.removeAll(map2.keySet());
        return map2;
    }

    private FetchOptions cloneOptionsWithoutCursors(final FetchOptions options) {
        final FetchOptions options2 = FetchOptions.Builder.withDefaults();
        if (options.getLimit() != null) {
            options2.limit(options.getLimit());
        }
        if (options.getOffset() != null) {
            options2.offset(options.getOffset());
        }
        if (options.getChunkSize() != null) {
            options2.chunkSize(options.getChunkSize());
        }
        if (options.getPrefetchSize() != null) {
            options2.prefetchSize(options.getPrefetchSize());
        }
        return options2;
    }

    protected void putAllCache(final Map<com.maintainer.data.provider.Key, Object> map) {
        putAllLocalCache(map);
        putAllMemcache(map);
    }

    protected void putAllMemcache(final Map<com.maintainer.data.provider.Key, Object> map) {
        if (nocache) return;

        final Map<String, Object> cacheable = new HashMap<String, Object>();
        for (final Entry<com.maintainer.data.provider.Key, Object> e : map.entrySet()) {
            cacheable.put(e.getKey().toString(), e.getValue());
        }

        memcache.putAll(cacheable);
    }

    protected void putAllLocalCache(final Map<com.maintainer.data.provider.Key, Object> map) {
        if (!local) {
            return;
        }

        for (final Entry<com.maintainer.data.provider.Key, Object> e : map.entrySet()) {
            putLocalCache(e.getKey(), e.getValue());
        }
    }

    protected void putCache(final com.maintainer.data.provider.Key key, final Object o) {
        putLocalCache(key, o);
        putMemcache(key, o);
    }

    protected void putMemcache(final com.maintainer.data.provider.Key key, final Object o) {
        if (nocache) return;

        memcache.put(key.toString(), o);
    }

    protected Object getCached(final com.maintainer.data.provider.Key key) throws Exception {
        if (nocache) return null;
        if (key == null) return null;

        Object o = getLocalCache(key);
        if (o == null) {
            //final Future<Object> future = memcache.get(key.toString());
            //o = future.get();
            o = memcache.get(key.toString());
            if (o != null) {
                putLocalCache(key, o);
            }
        }
        return o;
    }

    protected void invalidateCached(final com.maintainer.data.provider.Key key) {
        invalidateLocalCache(key);
        invalidateMemcache(key);
    }

    private void invalidateMemcache(final com.maintainer.data.provider.Key key) {
        if (nocache) return;

        memcache.delete(key.toString());
    }

    protected void putLocalCache(final com.maintainer.data.provider.Key key, final Object o) {
        if (local) {
//            cache.put(key.toString(), o);
        }
    }

    protected Object getLocalCache(final com.maintainer.data.provider.Key key) {
        if (!local) {
            return null;
        }

//        final Object o = cache.getIfPresent(key.toString());
//        return o;
        return null;
    }

    protected Map<com.maintainer.data.provider.Key, Object> getAllCache(final Collection<com.maintainer.data.provider.Key> keys) throws Exception {
        final Map<com.maintainer.data.provider.Key, Object> map = new LinkedHashMap<com.maintainer.data.provider.Key, Object>();
        if (nocache) return map;

        final Map<com.maintainer.data.provider.Key, Object> map2 = getAllLocalCache(keys);
        if (!map2.isEmpty()) {
            map.putAll(map2);
        }
        final Set<com.maintainer.data.provider.Key> keySet = map2.keySet();
        final boolean isChanged = keySet.isEmpty() || keys.removeAll(keySet);
        if (!keys.isEmpty() && isChanged) {

            final List<String> stringKeys = getStringKeys(keys);

            //final Future<Map<String, Object>> future = memcache.getAll(stringKeys);
            //final Map<String, Object> map3 = future.get();
            final Map<String, Object> map3 = memcache.getAll(stringKeys);
            if (map3 != null && !map3.isEmpty()) {
                for (final Entry<String, Object> e : map3.entrySet()) {
                    if (e.getValue() != null) {
                        map.put(com.maintainer.data.provider.Key.fromString(e.getKey()), e.getValue());
                    }
                }
            }
        }
        return map;
    }

    protected Map<com.maintainer.data.provider.Key, Object> getAllLocalCache(final Collection<com.maintainer.data.provider.Key> keys) {
        if (!local) {
            return Collections.emptyMap();
        }

//        final List<String> stringKeys = getStringKeys(keys);
//
//        final ImmutableMap<String, Object> allPresent = cache.getAllPresent(stringKeys);
//
//        final Map<com.maintainer.data.provider.Key, Object> keysPresent = new LinkedHashMap<com.maintainer.data.provider.Key, Object>();
//        for (final Entry<String, Object> e : allPresent.entrySet()) {
//            keysPresent.put(com.maintainer.data.provider.Key.fromString(e.getKey()), e.getValue());
//        }
//
//        return keysPresent;
        return Collections.emptyMap();
    }

    private List<String> getStringKeys(final Collection<com.maintainer.data.provider.Key> keys) {
        final List<String> stringKeys = new ArrayList<String>();
        for (final com.maintainer.data.provider.Key k : keys) {
            stringKeys.add(k.toString());
        }
        return stringKeys;
    }

    protected void invalidateLocalCache(final com.maintainer.data.provider.Key key) {
        if (local) {
//            cache.invalidate(key.toString());
        }
    }

    public static void writeBlob(final com.maintainer.data.provider.Key key, final byte[] bytes, final Integer version) throws Exception {
        final String folder = key.getKindName();
        final String name = (String) key.getId();
        final com.maintainer.data.provider.Key parent = key.getParent();
        writeBlob(folder, name, parent, bytes, version);
    }

    public static void writeBlob(final String folder, final String name, final byte[] bytes) throws Exception {
        writeBlob(folder, name, null, bytes, null);
    }

    public static void writeBlob(final String folder, final String name, final byte[] bytes, final Integer version) throws Exception {
        writeBlob(folder, name, null, bytes, version, true);
    }

    public static void writeBlob(final String folder, final String name, final com.maintainer.data.provider.Key parent, final byte[] bytes) throws Exception {
        writeBlob(folder, name, parent, bytes, null);
    }

    public static void writeBlob(final String folder, final String name, final com.maintainer.data.provider.Key parent, final byte[] bytes, final Integer version) throws Exception {
        writeBlob(folder, name, parent, bytes, version, true);
    }

    public static void writeBlob(final String folder, final Long id, final byte[] bytes) throws Exception {
        if (Utils.isEmpty(folder) ||  bytes == null) return;

        final Entity entity = createEntityWithOrWithoutParent(folder, id, null);

        writeBlob(entity, bytes, true);
    }

    public static void writeBlob(final String folder, final String name, final com.maintainer.data.provider.Key parent, final byte[] bytes, final Integer version, final boolean cache) throws Exception {
        if (Utils.isEmpty(folder) || Utils.isEmpty(name) || bytes == null) return;

        final Entity entity = createEntityWithOrWithoutParent(folder, name, parent);

        writeBlob(entity, bytes, cache);
    }

    private static Entity createEntityWithOrWithoutParent(final String folder, final String name, final com.maintainer.data.provider.Key parent) {
        Entity entity = null;
        if (parent != null) {
            final Key parentKey = createDatastoreKey(parent);
            entity = new Entity(folder, name, parentKey);
        } else {
            entity = new Entity(folder, name);
        }
        return entity;
    }

    private static Entity createEntityWithOrWithoutParent(final String folder, final Long id, final com.maintainer.data.provider.Key parent) {
        Entity entity = null;
        if (parent != null) {
            final Key parentKey = createDatastoreKey(parent);
            entity = new Entity(folder, id, parentKey);
        } else {
            entity = new Entity(folder, id);
        }
        return entity;
    }

    private static void writeBlob(final Entity entity, final byte[] bytes, final boolean cache) throws Exception {
        writeBlob(entity, bytes, null, cache);
    }

    private static void writeBlob(final Entity entity, final byte[] bytes, final Integer version, final boolean cache) throws Exception {
        // log.warning("Undeflated bytes: " + bytes.length);
        byte[] deflated = deflate(bytes);

//        int bytesRemaining = deflated.length;
//        int count = 0;
//        while (bytesRemaining > 0) {
//            int length = bytesRemaining > 1048503? 1048503 : bytesRemaining;
//            byte[] dest = new byte[length];
//            System.arraycopy(deflated, 1048503 * count, dest, 0, length);
//            final Blob blob = new Blob(dest);
//            String name = "content" + (count > 0?count:"");
//            entity.setUnindexedProperty(name, blob);
//            bytesRemaining -= 1048503;
//            count++;
//        }
//
//        String name = "content" + (count > 0?count:"");
//        while (entity.getProperty(name) != null) {
//            entity.removeProperty(name);
//            count++;
//            name = "content" + (count > 0?count:"");
//        }

        log.warning("Deflated bytes: " + deflated.length);
        entity.setUnindexedProperty("content", new Blob(deflated));
        entity.setUnindexedProperty("length", deflated.length);
        entity.setUnindexedProperty("encoding", "zip");

        final Key datastoreKey = entity.getKey();
        final Key parent = datastoreKey.getParent();
        if (parent != null) {
            final com.maintainer.data.provider.Key key = createNobodyelsesKey(parent);
            final String parentName = key.asString();
            entity.setUnindexedProperty("owner", parentName);
        }

        if (version != null) {
            entity.setUnindexedProperty("version", version);
        }

        // TODO: This is what is wrong. We must use the synchronous put because the next write
        // will happen before the write completes. Need to change the process to write to sub-indexes
        // first, then write to the main index when all is done.
        DatastoreServiceFactory.getDatastoreService().put(entity);

        final String keyToString = datastoreKey.toString();

        if (cache) {
            try {
                MyMemcacheServiceFactory.getMemcacheService().put(keyToString, deflated);
            } catch(Exception e) {
                MyMemcacheServiceFactory.getMemcacheService().delete(keyToString);
            }
        } else {
            MyMemcacheServiceFactory.getMemcacheService().delete(keyToString);
        }
    }

    public static com.maintainer.data.provider.datastore.Blob readBlob(final String folder, final Long id) throws Exception {
        final Key key = createDatastoreKey(folder, id);
        return readBlob(key);
    }

    public static com.maintainer.data.provider.datastore.Blob readBlob(final String folder, final String name) throws Exception {
        return readBlob(null, folder, name);
    }

    public static com.maintainer.data.provider.datastore.Blob readBlob(final com.maintainer.data.provider.Key parent, final String folder, final String name) throws Exception {
        final Key key = createDatastoreKeyWithOrWithoutParent(parent, folder, name);
        return readBlob(key);
    }

    private static Key createDatastoreKeyWithOrWithoutParent(final com.maintainer.data.provider.Key parent, final String folder, final String name) {
        Key key = null;

        if (parent != null) {
            key = createDatastoreKey(parent, folder, name);
        } else {
            key = createDatastoreKey(folder, name);
        }
        return key;
    }

    public static String getKeyStringWithOrWithoutParent(final com.maintainer.data.provider.Key parent, final String folder, final String name) {
        Key key = null;

        if (parent != null) {
            key = createDatastoreKey(parent, folder, name);
        } else {
            key = createDatastoreKey(folder, name);
        }
        return key.toString();
    }

    public static com.maintainer.data.provider.datastore.Blob readBlob(final Key key) throws Exception {
        final String keyToString = key.toString();
        // log.warning("Reading: " + keyToString);
        byte[] deflated = null;
        byte[] inflated = null;

        Object cached = MyMemcacheServiceFactory.getMemcacheService().get(keyToString);
        // log.warning("cached = " + cached);
        if (cached != null) {
            deflated = (byte[]) cached;
            try {
            	deflated = inflate(deflated);
            } catch (Exception e) {
            	log.severe(e.getMessage());

            	StringBuilder buf = new StringBuilder();
            	buf.append('[');
            	for (int i = 0; i < 20; i++) {
            		if (deflated.length < i) {
            			break;
            		}
            		buf.append(deflated[i]);
            		buf.append(", ");
            	}
            	buf.append(']');
            	log.severe(buf.toString());
            }
            // log.warning("deflated.length = " + deflated.length);
        }
        long length = 0;

        try {
            if (deflated == null || deflated.length == 0) {
                final Entity entity = DatastoreServiceFactory.getDatastoreService().get(key);
                final String encoding = (String) entity.getProperty("encoding");

                // log.warning("Encoding: " + encoding);
                if ("json".equals(encoding)) {
                    length = (Long) entity.getProperty("length");
                    deflated = new byte[(int) length];
                    int bytesRemaining = (int) length;
                    int count = 0;
                    while (bytesRemaining > 0) {
                        final String name = "content" + (count > 0?count:"");
                        Blob blob = (Blob) entity.getProperty(name);
                        byte[] src = blob.getBytes();
                        System.arraycopy(src, 0, deflated, 1048503 * count, src.length);
                        bytesRemaining -= src.length;
                    }
                } else if ("zip".equals(encoding)) {
                    // log.warning("Reading zip...");
                    Blob blob = (Blob) entity.getProperty("content");
                    deflated = blob.getBytes();
                    // log.warning("Retrieved bytes: " + deflated.length);
                    inflated = inflate(deflated);
                    length = inflated.length;
                    // log.warning("Inflated bytes: " + length);
                } else {
                    Blob blob = (Blob) entity.getProperty("content");
                    deflated = blob.getBytes();
                    length = deflated.length;
                }

                try {
                    MemcacheServiceFactory.getMemcacheService().put(keyToString, deflated);
                } catch (Exception e) {
                    MemcacheServiceFactory.getMemcacheService().delete(keyToString);
                }
            } else {
                inflated = deflated;
                length = inflated.length;
            }

            com.maintainer.data.provider.datastore.Blob blob2 = new com.maintainer.data.provider.datastore.Blob(inflated);

            // log.warning(MessageFormat.format("Retrieving index {0} results in {1} bytes.", keyToString, length));

            return blob2;
        } catch (final EntityNotFoundException e) {}

        return null;
    }

    public static void deleteBlob(final String folder, final String name) {
        deleteBlob(null, folder, name);
    }

    public static void deleteBlob(final com.maintainer.data.provider.Key parent, final String folder, final String name) {
        final Key key = createDatastoreKeyWithOrWithoutParent(parent, folder, name);
        deleteBlob(key);
    }

    public static void deleteBlob(final Key key) {
        DatastoreServiceFactory.getAsyncDatastoreService().delete(key);
        final String keyToString = KeyFactory.keyToString(key);
        MyMemcacheServiceFactory.getAsyncMemcacheService().delete(keyToString);
    }

    public static byte[] inflate(final byte[] bytes) throws Exception {
        Inflater decompresser = new Inflater();
        decompresser.setInput(bytes);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        while ((!decompresser.finished())) {
            byte[] buff=new byte[1024];
            int count=decompresser.inflate(buff);
            baos.write(buff,0,count);
        }
        byte[] output = baos.toByteArray();
        baos.close();
        decompresser.end();
        return output;
    }

    public static byte[] deflate(byte[] bytes) {
        final byte[] output = new byte[1048576];
        final Deflater compresser = new Deflater();
        compresser.setInput(bytes);
        compresser.finish();
        final int length = compresser.deflate(output);
        compresser.end();

        final byte[] dest = new byte[length];
        System.arraycopy( output, 0, dest, 0, length );
        return dest;
    }
}
