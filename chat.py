import chainlit as cl
import litellm
from langchain.memory import ConversationBufferMemory
from chainlit.types import ThreadDict
from langchain_community.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.schema.output_parser import StrOutputParser
from langchain.schema.runnable import Runnable, RunnablePassthrough, RunnableLambda
from langchain.schema.runnable.config import RunnableConfig

def setup_runnable():
    memory = cl.user_session.get("memory")  # type: ConversationBufferMemory
    model = ChatOpenAI(streaming=True)
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "You are a helpful chatbot"),
            MessagesPlaceholder(variable_name="history"),
            ("human", "{question}"),
        ]
    )

    runnable = (
        RunnablePassthrough.assign(
            history=RunnableLambda(memory.load_memory_variables) | itemgetter("history")
        )
        | prompt
        | model
        | StrOutputParser()
    )
    cl.user_session.set("runnable", runnable)

@cl.on_chat_start
def start_chat():
  system_message = {
    "role": "system", 
    "content": "You are a helpful assistant who tries their best to answer questions.Your goal is to provide accurate and useful information to the best of your ability, based on your training data and browse internet to get latest information. Please answer questions, offer explanations, and assist with tasks to the best of your capabilities"
  }
  cl.user_session.set("message_history", [system_message])

@cl.on_chat_resume
async def on_chat_resume(thread: ThreadDict):
    memory = ConversationBufferMemory(return_messages=True)
    root_messages = [m for m in thread["steps"] if m["parentId"] == None]
    for message in root_messages:
        if message["type"] == "user_message":
            memory.chat_memory.add_user_message(message["output"])
        else:
            memory.chat_memory.add_ai_message(message["output"])

    cl.user_session.set("memory", memory)
    setup_runnable()

@cl.on_message
async def on_message(message: cl.Message):
  messages = cl.user_session.get("message_history")
  if len(message.elements) > 0:
    for element in message.elements:
      with open(element.path, "r") as uploaded_file:
        content = uploaded_file.read()        
      messages.append({"role": "user", "content": content})
      confirm_message = cl.Message(content=f"Uploaded file: {element.name}")
      await confirm_message.send()



  msg = cl.Message(content="")
  await msg.send()
  
  messages.append({"role": "user", "content": message.content})

  response = await litellm.acompletion(
    model="ollama/llama3",
    temparature=0,
    messages = messages,
    api_base="http://localhost:11434",
    stream=True
  )

  

  async for chunk in response:
    if chunk:
      content = chunk.choices[0].delta.content
      if content:
        await msg.stream_token(content)

  messages.append({"role": "assistant", "content": msg.content})
  await msg.update()