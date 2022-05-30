CREATE TABLE jobs (
    id INT UNSIGNED auto_increment PRIMARY KEY,
    status ENUM('idle', 'processing', 'error', 'done'),
    worker VARCHAR(15),
    strategy VARCHAR(30),
    strategy_version INT UNSIGNED,
    asset CHAR(6),
    year INT UNSIGNED,
    timeframe CHAR(3),
    params TEXT,
    message VARCHAR(255),
    execution_time INT UNSIGNED
) engine=InnoDB default charset latin1;

CREATE TABLE results (
    job_id INT UNSIGNED PRIMARY KEY,
    orders INT UNSIGNED,
    winning_ratio DOUBLE UNSIGNED,
    net_profit DOUBLE,
    average_gain DOUBLE,
    average_loss DOUBLE,
    profit_factor DOUBLE
) engine=InnoDB default charset latin1;

CREATE TABLE trading_cost (
    asset CHAR(6) PRIMARY KEY,
    cost DOUBLE UNSIGNED
) engine=InnoDB default charset latin1;
