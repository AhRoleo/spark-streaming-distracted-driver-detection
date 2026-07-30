import tf2onnx
import tensorflow as tf

model = tf.keras.models.load_model("baseline_cnn.keras")
tf2onnx.convert.from_keras(model, output_path="3DVGG19onnx")