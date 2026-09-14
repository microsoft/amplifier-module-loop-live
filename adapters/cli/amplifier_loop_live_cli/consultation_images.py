"""Add the missing ImageBlock conversion to the pinned ChatGPT driver.

OAuth, endpoints, model controls, parsing and retry semantics stay in the driver.
This extension is scoped to the isolated consultation session.
"""
from amplifier_core.message_models import ImageBlock
from amplifier_module_provider_openai_chatgpt import ChatGPTProvider


class ImageChatGPTProvider(ChatGPTProvider):
    @classmethod
    def wrap(cls, original):
        provider=cls.__new__(cls)
        # Share lifecycle-owned state: the original mount cleanup closes the
        # same lazy HTTP client and retains any refreshed OAuth token state.
        provider.__dict__=original.__dict__
        return provider

    def _convert_content(self, content, role="user"):
        if isinstance(content,str): return super()._convert_content(content,role)
        result=[]
        for block in content:
            converted=super()._convert_content([block],role)
            if converted or not isinstance(block,ImageBlock):
                result.extend(converted);continue
            source=block.source
            if role!="user" or source.get("type")!="base64" or source.get("media_type") not in {"image/png","image/jpeg","image/webp"}:
                raise ValueError("Consultation requires user-selected PNG, JPEG or WebP bytes")
            result.append({"type":"input_image","image_url":f"data:{source['media_type']};base64,{source['data']}","detail":"auto"})
        return result
