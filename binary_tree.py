class Node:
    def __init__(self, value):
        self.value = value
        self.left = None
        self.right = None

class BinaryTree:
    def __init__(self):
        self.root = None

    def __str__(self):
        return self._str_recursive(self.root, 0)

    def _str_recursive(self, current_node, level):
        if current_node:
            left_str = self._str_recursive(current_node.left, level + 1)
            current_str = "  " * level + str(current_node.value) + "\n"
            right_str = self._str_recursive(current_node.right, level + 1)
            return left_str + current_str + right_str
        else:
            return ""

    def insert(self, value):
        if not self.root:
            self.root = Node(value)
        else:
            self._insert_recursive(self.root, value)

    def _insert_recursive(self, current_node, value):
        if value < current_node.value:
            if current_node.left:
                self._insert_recursive(current_node.left, value)
            else:
                current_node.left = Node(value)
        else:
            if current_node.right:
                self._insert_recursive(current_node.right, value)
            else:
                current_node.right = Node(value)

    def print_leaves(self):
        self._print_leaves_recursive(self.root)

    def _print_leaves_recursive(self, current_node):
        if current_node:
            if not current_node.left and not current_node.right:
                print(current_node.value)
            else:
                self._print_leaves_recursive(current_node.left)
                self._print_leaves_recursive(current_node.right)

    def traverse_level_order(self):
        level_contents = {}  # Dictionary to store level contents
        self._traverse_level_order_recursive(self.root, 0, level_contents)
        return level_contents

    def _traverse_level_order_recursive(self, current_node, level, level_contents):
        if current_node:
            if level not in level_contents:
                level_contents[level] = []
            level_contents[level].append(current_node.value)
            self._traverse_level_order_recursive(current_node.left, level + 1, level_contents)
            self._traverse_level_order_recursive(current_node.right, level + 1, level_contents)


def generate_position_structure(levels):
    position_structure = {}
    delta = 4
    for level in range(levels):
        num_nodes = 2 ** level
        center = 2 ** (levels - level - 1)
        positions = [center + delta * (i - num_nodes // 2) for i in range(num_nodes)]
        position_structure[f'level {level}'] = positions

    return position_structure


def main():
    # Create a binary tree
    tree = BinaryTree()
    tree.insert(5)
    tree.insert(3)
    tree.insert(8)
    tree.insert(2)
    tree.insert(4)
    tree.insert(7)
    tree.insert(9)

    print("********************************************")
    # Traverse the tree level by level and store contents in a dictionary
    level_contents = tree.traverse_level_order()

    # Print out the leaves of the binary tree using recursion
    # print("Leaves of the binary tree:")
    # tree.print_leaves()
    for level, contents in level_contents.items():
        if level == 0:
            print(f'root: {contents[0]}')
        else:
            print(f'level {level}: {contents}')
    print("********************************************")
    print(level_contents)
    print("********************************************")

    for level, contents in level_contents.items():
        if level == 0:
            print(f'root: {contents[0]}')
        else:
            print(f'level {level}: {contents}')

    num_levels = len(level_contents)
    positions_dict = generate_position_structure(num_levels)

    for level, level_positions in positions_dict.items():
        indentation = ' ' * (level_positions[0] * 2)  # Calculate indentation based on position
        if level == 0:
            print(f'root: {level_contents[level][0]}')
        else:
            contents_str = ', '.join(map(str, level_contents[level]))
            print(f'{indentation}level {level}: [{contents_str}]')


if __name__ == "__main__":
    main()
