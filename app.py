import sys
from PySide6.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel
from PySide6.QtGui import QPainter, QColor, QBrush
from PySide6.QtCore import Qt, QThread, Signal, QSize
from confluent_kafka import Consumer, KafkaError

class Square(QWidget):
    def __init__(self, color=QColor("red")):
        super().__init__()
        self.color = color

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.fillRect(event.rect(), QBrush(self.color))

    def set_color(self, color):
        self.color = color
        self.update()

class KafkaConsumer(QThread):
    message_received = Signal(str)
    error_occurred = Signal(str)

    def __init__(self, bootstrap_servers, group_id, topic):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.running = False

    def run(self):
        self.running = True
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([self.topic])

        while self.running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    self.error_occurred.emit(f'Error: {msg.error()}')
                    break

            message = msg.value().decode("utf-8")
            self.message_received.emit(message)

        consumer.close()

    def stop(self):
        self.running = False

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.bootstrap_servers = 'localhost:9092'
        self.group_id = 'test-group'
        self.topic = 'test'

        main_widget = QWidget()
        self.setCentralWidget(main_widget)

        layout = QHBoxLayout()
        main_widget.setLayout(layout)

        # Left side menu
        left_menu_widget = QWidget()
        left_menu_layout = QVBoxLayout(left_menu_widget)
        left_menu_layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(left_menu_widget, 0)

        self.start_button = QPushButton("Start Kafka Consumer")
        self.putaway_button = QPushButton("Putaway")
        self.pickup_button = QPushButton("Pickup")
        self.location_transfer_button = QPushButton("Location Transfer")

        left_menu_layout.addWidget(self.start_button)
        left_menu_layout.addWidget(self.putaway_button)
        left_menu_layout.addWidget(self.pickup_button)
        left_menu_layout.addWidget(self.location_transfer_button)

        self.status_label = QLabel("Consumer Status: Not Started")
        left_menu_layout.addWidget(self.status_label)

        self.error_label = QLabel()
        left_menu_layout.addWidget(self.error_label)

        # Set fixed width for the left menu
        left_menu_width = 200
        left_menu_widget.setFixedWidth(left_menu_width)

        # Right side content
        right_content_layout = QVBoxLayout()
        layout.addLayout(right_content_layout)

        hbox = QHBoxLayout()
        right_content_layout.addLayout(hbox)

        self.squares = []
        for _ in range(8):
            square = Square()
            hbox.addWidget(square)
            self.squares.append(square)

        self.kafka_consumer = KafkaConsumer(self.bootstrap_servers, self.group_id, self.topic)
        self.kafka_consumer.message_received.connect(self.update_colors)
        self.kafka_consumer.error_occurred.connect(self.display_error)

        self.start_button.clicked.connect(self.start_consumer)
        self.putaway_button.clicked.connect(lambda: self.print_button_clicked("Putaway"))
        self.pickup_button.clicked.connect(lambda: self.print_button_clicked("Pickup"))
        self.location_transfer_button.clicked.connect(lambda: self.print_button_clicked("Location Transfer"))

    def start_consumer(self):
        if not self.kafka_consumer.isRunning():
            self.kafka_consumer.start()
            self.status_label.setText("Consumer Status: Running")
            self.start_button.setEnabled(False)

    def update_colors(self, message):
        colors = [QColor("red") if digit == '0' else QColor("green") for digit in message]
        for square, color in zip(self.squares, colors):
            square.set_color(color)

    def display_error(self, error_message):
        self.error_label.setText(error_message)

    def print_button_clicked(self, button_text):
        print(f"{button_text} button clicked")

    def closeEvent(self, event):
        if self.kafka_consumer.isRunning():
            self.kafka_consumer.stop()
            self.kafka_consumer.wait()

        event.accept()

app = QApplication(sys.argv)
window = MainWindow()
window.show()
sys.exit(app.exec())