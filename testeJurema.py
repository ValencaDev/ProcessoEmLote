from transformers import AutoModelForCausalLM, AutoTokenizer
import torch

model = AutoModelForCausalLM.from_pretrained("Jurema-br/Jurema-7B", device_map="auto", torch_dtype=torch.bfloat16)
tokenizer = AutoTokenizer.from_pretrained("Jurema-br/Jurema-7B")

prompt = "O que significa fazer justiça em um Estado Democrático de Direito como o brasileiro?"
input_ids = tokenizer(prompt, return_tensors="pt").input_ids.to(model.device)
output = model.generate(input_ids, max_new_tokens=512)
print(tokenizer.decode(output[0], skip_special_tokens=True))