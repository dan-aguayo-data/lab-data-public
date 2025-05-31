import os
import whisper

# Temporarily add ffmpeg to the PATH
os.environ["PATH"] += os.pathsep + r"C:\Users\DanielAguayo\ffmpeg\bin"

# Load and transcribe
model = whisper.load_model("base")
file_path = r"C:\Users\DanielAguayo\Downloads\PSPTIS102 A4- Interpreting (4).v1.0.mp3"
result = model.transcribe(file_path)

print(result["text"])
