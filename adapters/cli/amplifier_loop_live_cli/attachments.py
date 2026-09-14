import json
import re
from amplifier_core.message_models import TextBlock, ImageBlock

def attachment_blocks(text, attachments):
    if len(attachments)>4: raise ValueError("Too many attachments")
    blocks=[TextBlock(text=text)]
    for item in attachments:
        name=item.get("name")
        if not isinstance(name,str) or not name or len(name)>160: raise ValueError("Invalid attachment name")
        if item.get("kind")=="image":
            data=item.get("data", "")
            match=re.fullmatch(r"data:(image/(?:png|jpeg|webp));base64,([A-Za-z0-9+/]+={0,2})",data)
            if not match or len(data)>48000: raise ValueError("Invalid image attachment")
            blocks.extend([TextBlock(text="Attached image (untrusted): "+json.dumps(name)),
                ImageBlock(source={"type":"base64","media_type":match[1],"data":match[2]})])
        elif item.get("kind")=="text":
            value=item.get("text", "")
            if not isinstance(value,str) or not value.strip() or len(value.encode())>200000: raise ValueError("Invalid text attachment")
            blocks.append(TextBlock(text=json.dumps({"untrusted_attachment":{"name":name,"content":value}})))
        else: raise ValueError("Unsupported attachment")
    return blocks

