package play.modules.elasticsearch;

import org.hibernate.Hibernate;
import org.hibernate.proxy.HibernateProxy;
import play.Logger;
import play.db.Model;
import play.db.Model.Factory;
import play.db.jpa.JPA;
import play.jobs.Job;
import play.modules.elasticsearch.mapping.MappingUtil;

import javax.persistence.metamodel.ManagedType;
import java.util.List;
import java.util.Set;

public class ReindexDatabaseJob extends Job<Void> {

    // Fetch chunks of this size from DB
    private static final int PAGE_SIZE = 256;

    private final ElasticSearchDeliveryMode deliveryMode;

    /**
     * Default constructor which creates a job which reindexes all entities in the database in one thread.
     */
    public ReindexDatabaseJob() {
        // Do the whole task in a single thread by default
        this(null);
    }

    /**
     * Constructor which allows you to specify your own delivery mode for reindexing.
     *
     * @param deliveryMode set null to use default {@link ElasticSearchDeliveryMode.SYNCHRONOUS}
     */
    public ReindexDatabaseJob(final ElasticSearchDeliveryMode deliveryMode) {
        super();
        if (deliveryMode == null) {
            this.deliveryMode = ElasticSearchDeliveryMode.SYNCHRONOUS;
        } else {
            this.deliveryMode = deliveryMode;
        }
    }


    /**
     * Initialize proxy object
     *
     * @param entity
     * @param <T>
     * @return
     */
    public static <T> T initializeAndUnproxy(T entity) {
        if (entity == null) {
            throw new
                    NullPointerException("Entity passed for initialization is null");
        }

        Hibernate.initialize(entity);
        if (entity instanceof HibernateProxy) {
            entity = (T) ((HibernateProxy) entity).getHibernateLazyInitializer()
                    .getImplementation();
        }
        return entity;
    }

    @Override
    public void doJob() throws Exception {
        final Set<ManagedType<?>> types = JPA.em().getMetamodel().getManagedTypes();
        for (final ManagedType managedType : types) {
            final Class modelClass = managedType.getJavaType();
            // Proceed only if searchable
            if (!MappingUtil.isSearchable(modelClass)) {
                continue;
            }

            final Factory factory = Model.Manager.factoryFor(modelClass);
            final long count = factory.count(null, null, null);

            // Defensively avoid overflow leading to infinite loop below
            if (count > Integer.MAX_VALUE) {
                throw new RuntimeException(String.format("Number of entities %s to index is too large (%d)", modelClass, count));
            }

            Logger.info("Reindexing %s entities of type %s", count, modelClass);

            long offset = 0;
            // loop over pages
            while (offset < count) {
                final List results = factory.fetch((int) offset, PAGE_SIZE, null, null, null, null, null);
                // loop over individual entities within one page
                for (final Object o : results) {
                    ElasticSearch.index((Model) initializeAndUnproxy(o), deliveryMode);
                }
                offset += PAGE_SIZE;
            }
        }
    }

}
