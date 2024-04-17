from pydantic import BaseModel
from pydantic_core import PydanticUndefined
from ruamel.yaml import YAML

def _gen_config(struct: BaseModel):
    """Recursively generate a config dictionary from a Pydantic model"""
    model_dict = {}

    for field, info in struct.model_fields.items():
        default = info.get_default(call_default_factory = True) or None
        
        if default == PydanticUndefined:
            default = None
        
        if not default and info.annotation:
            try:
                default = info.annotation.model_construct()
            except:
                pass
        
        if isinstance(default, list):
            nd = []

            for i in default:
                if isinstance(i, BaseModel):
                    nd.append(_gen_config(i))
                else:
                    nd.append(i) 
        
            model_dict[field] = nd
        elif isinstance(default, dict):
            nd = {}

            for k, v in default.items():
                if isinstance(v, BaseModel):
                    nd[k] = _gen_config(v)
                else:
                    nd[k] = v

            model_dict[field] = nd
        elif isinstance(default, BaseModel):
            model_dict[field] = _gen_config(default)
        else:
            if getattr(struct, field, None) is not None:
                model_dict[field] = getattr(struct, field)
            else:
                model_dict[field] = default
    
    return model_dict
        
def gen_config(struct: BaseModel, outp_file: str):
    # Convert to yaml
    yaml = YAML()

    with open(outp_file, 'w') as f:
        yaml.dump(_gen_config(struct), f)
