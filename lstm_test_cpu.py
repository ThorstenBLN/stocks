import tensorflow as tf

print(tf.config.list_physical_devices("GPU"))
# categorical model
with tf.device('/GPU:0'): #device:
    print(tf.config.list_physical_devices("GPU"))