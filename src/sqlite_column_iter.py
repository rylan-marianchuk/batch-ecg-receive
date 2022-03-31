
class SqliteColumnIter:

    def __init__(self, column_containers=None):
        if column_containers is not None:
            self.refill_containers(column_containers)

    def refill_containers(self, column_containers):
        """

        :param column_containers: (tuple of container objects) first element is first column (Primary Key), second
                                  element holds data for the second column
        Must assume all columns have at least the length of first element (primary key container), excess elements ignored
        """
        self.column_containers = column_containers
        self.i = 0
        self.container_size = len(column_containers[0])
        self.columns = len(column_containers)


    def __iter__(self):
        return self


    def __next__(self):
        if self.i >= self.container_size:
            raise StopIteration
        L = list(self.column_containers[x][self.i] for x in range(self.columns))
        self.i += 1
        return L

