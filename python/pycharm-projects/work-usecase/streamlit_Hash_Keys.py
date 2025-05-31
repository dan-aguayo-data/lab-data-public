import pickle
from pathlib import Path
import streamlit_authenticator as stauth

hashed_passwords = stauth.Hasher(['rupamp_zpkxle_25!']).generate()
print(hashed_passwords)

#pip install streamlit-authenticator==0.1.5
