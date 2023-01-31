from google.cloud import pubsub_v1

# Your Google Cloud project ID
project_id = "elevated-column-376015"

# The name of the Pub/Sub topic to which you want to publish
topic_name = "prueba_send_1"

# Set up the Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# Publish the message and wait for it to be completed
message = "Hello, Pub/Sub2!"
future = publisher.publish(topic_path, data=message.encode("utf-8"))
result = future.result()

# The message has been successfully published
print("Message published with ID: {}".format(result))
