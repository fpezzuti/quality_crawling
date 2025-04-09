import datetime

class Component:
   
    component_name = "LOG"

    def __init__(self, verbose: bool, verbosity: int = 2) -> None:
        self.verbose = verbose
        self.verbosity = verbosity
        
        
    def log(self, msg: str, priority: int = 2) -> None:
        if self.verbose and priority <= self.verbosity:
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{self.component_name}][{current_time}]: {msg}")