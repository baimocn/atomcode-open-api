"""
models - 模型名称映射

将 AtomCode 专属模型别名映射为网关支持的全名。
AtomCode 的短名称（如 deepseek-v4-flash）在网关上会触发"AtomCode 独享"限制，
使用全名（如 deepseek-ai/DeepSeek-V4-Flash）可以绕过。
"""

from typing import Dict, Optional


# 手动维护的别名映射表（作为缓存未命中时的后备）
KNOWN_ALIASES: Dict[str, str] = {
    # DeepSeek 系列
    "deepseek-v4-flash": "deepseek-ai/DeepSeek-V4-Flash",
    "deepseek-v4-pro": "deepseek-ai/DeepSeek-V4-Pro",
    "deepseek-v4": "deepseek-ai/DeepSeek-V4-Flash",  # 默认映射到 Flash
    "deepseek-v3": "deepseek-ai/DeepSeek-V3",
    "deepseek-v3.2": "deepseek-ai/DeepSeek-V3.2",
    "deepseek-r1": "deepseek-ai/DeepSeek-R1",
    "deepseek-r1-0528": "deepseek-ai/DeepSeek-R1-0528",
    # GLM 系列
    "GLM-5": "zai-org/GLM-5",
    "GLM-5.1": "zai-org/GLM-5.1",
}


class ModelMapper:
    """模型名称映射器"""

    def __init__(self):
        self._aliases: Dict[str, str] = dict(KNOWN_ALIASES)
        self._all_models: list = []

    def update_from_api(self, models_data: Dict) -> int:
        """
        从 /v1/models 响应更新映射表

        Args:
            models_data: /v1/models 的 JSON 响应

        Returns:
            可用模型数量
        """
        self._all_models = []
        for model in models_data.get("data", []):
            model_id = model.get("id", "")
            if model_id:
                self._all_models.append(model_id)
                # 全名到自身的映射
                self._aliases[model_id] = model_id
                # 短名称映射（/ 后面的部分）
                if "/" in model_id:
                    short = model_id.split("/")[-1]
                    if short not in self._aliases:
                        self._aliases[short] = model_id
        return len(self._all_models)

    def resolve(self, model_name: str) -> str:
        """
        解析模型名称

        Args:
            model_name: 用户请求的模型名称（可能是短名称或全名）

        Returns:
            网关支持的模型全名
        """
        # 精确匹配
        if model_name in self._aliases:
            return self._aliases[model_name]

        # 大小写不敏感匹配
        lower = model_name.lower()
        for alias, full in self._aliases.items():
            if alias.lower() == lower:
                return full

        # 部分匹配（用户输入包含在某个模型名中）
        for model_id in self._all_models:
            if lower in model_id.lower():
                return model_id

        # 原样返回
        return model_name

    @property
    def available_models(self) -> list:
        """所有可用模型 ID 列表"""
        return list(self._all_models)

    def get_openai_models_response(self) -> Dict:
        """
        构造 OpenAI 格式的 /v1/models 响应

        Returns:
            标准 OpenAI models 响应 JSON
        """
        return {
            "object": "list",
            "data": [
                {
                    "id": model_id,
                    "object": "model",
                    "created": 0,
                    "owned_by": "atomcode-codingplan",
                }
                for model_id in self._all_models
            ],
        }
