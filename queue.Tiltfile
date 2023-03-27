load('ext://helm_resource', 'helm_resource', 'helm_repo')

def validate_queue(queue_type):
    if queue_type not in ['kafka', 'pubsublite', 'noop']:
        fail("invalid queue_type specified: {}".format(queue_type))

def provision_kafka(path='.', namespace='kafka', topics=[], partitions=5):
    QUEUE_NAME='kafka'
    kafka_chart_path = os.path.join(path, 'infra/k8s/kafka')
    k8s_yaml(local(
        'helm template {} --set-json=\'topics={}\' --set=partitions={} --set=namespace={} --set=cluster={}'.format(
            kafka_chart_path, topics, partitions, namespace, QUEUE_NAME,
        ),
    ))
    watch_file(kafka_chart_path)
    helm_repo('strimzi', 'https://strimzi.io/charts/')
    helm_resource('kafka-operator',
        'strimzi/strimzi-kafka-operator', 
        namespace=QUEUE_NAME,
        labels=QUEUE_NAME
    )
    k8s_resource(
        objects=["{}:Kafka:{}".format(QUEUE_NAME, namespace)],
        new_name=QUEUE_NAME,
        pod_readiness='wait',
        extra_pod_selectors=[{'strimzi.io/controller-name': 'kafka-kafka'}],
        discovery_strategy='selectors-only',
        labels=QUEUE_NAME
    )

def provision_pubsublite(path, topics=[], create_subscription=False):
    os.putenv("TF_VAR_topics", "{}".format(topics))
    os.putenv("TF_VAR_prefix", "dev-{}".format(os.getenv("USER")))
    os.putenv("TF_VAR_create_subscription", "{}".format(create_subscription))
    if config.tilt_subcommand == "up":
        print("provisioning pubsublite resources...")
        print(local("make -C {} apply".format(path)))
