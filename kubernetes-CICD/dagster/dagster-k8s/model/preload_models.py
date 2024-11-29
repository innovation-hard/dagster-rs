from transformers import AutoTokenizer, AutoModelForTokenClassification
from huggingface_hub import hf_hub_download
print('Loading models')
transformers_model_default = "51la5/roberta-large-NER"
tokenizer = AutoTokenizer.from_pretrained(transformers_model_default, model_max_length=512)
del tokenizer
print('Downloading from hub')
hf_hub_download(transformers_model_default, 'pytorch_model.bin')
print('Finish Loading models')