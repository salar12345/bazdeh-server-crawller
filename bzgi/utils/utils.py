from ncl.utils.common.singleton import Singleton
from ncl.utils.helper.commons_utils import CommonsUtils


class Utils(CommonsUtils, metaclass=Singleton):
    def __init__(self):
        super().__init__()
