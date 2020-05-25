import re

text = ''
with open("flinktutorial.md", "r") as f:
    text = f.read()
    text = text.replace("[source,shell]\n----", "```{.sh}")
    text = text.replace("[source,scala]\n----", "```{.scala}")
    text = text.replace("[source,xml]\n----", "```{.xml}")
    text = text.replace("----", "```")
    text = text.replace("[source,scala]", "")
    text = text.replace("```", "asdfasdf")
    text = text.replace("``", "`")
    text = text.replace("asdfasdf", "```")
    p = re.findall("image::(.*?)\[\]", text)
    for s in p:
        text = text.replace("image::" + s + "[]", "![]" + "(" + s + ")")

with open("flinktutorial.md", "w") as f:
    f.write(text)