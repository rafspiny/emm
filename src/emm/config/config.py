import os
from dotenv import load_dotenv

class Config():

    emm_db_host: str
    emm_db_port: str
    emm_db_user: str
    emm_db_pwd: str
    emm_db: str

    def __init__(self) -> None:
        self.emm_db_host = None
        self.emm_db_port = None
        self.emm_db_user = None
        self.emm_db_pwd = None
        self.emm_db = None
        self._initialize_properties()

    def _initialize_properties(self):
        emm_config_file_name = os.getenv('EMM_CONFIG')
        emm_config_path = os.sep.join([os.getcwd(), "src", "emm", "config", "env", f"{emm_config_file_name}.env"])
        if emm_config_path and os.path.exists(emm_config_path):
            load_dotenv(emm_config_path)
        else:
            raise FileNotFoundError(f"The configuration file '{emm_config_path}' does not exist.")            

        for field in self.__dict__.keys():
            setattr(self, field, os.environ[field.upper()])


config: Config = Config()