from dataclasses import dataclass, field
from format_currency import from_satoshi
from sqlalchemy import ForeignKey, Integer, MetaData, String, Table, Column, PrimaryKeyConstraint, DECIMAL, BigInteger
from sqlalchemy.orm import registry, relationship


mapper_registry = registry()


# entity
@dataclass
class Blockchain:
    blockchain_id: int
    name: str
    short_name: str
    short_name_lower: str = None

    def __post_init__(self):
        self.short_name_lower = self.short_name.lower()


# entity
@dataclass
class ExcludedAddress:
    value: str
    blockchain_id: int


@dataclass
class Token:
    name: str
    short_name: str
    address: str
    blockchain: Blockchain
    min_holder_balance: int = 1000
    decimals: int = 8


# entity
@dataclass(order=True)
class AddressBalance:
    address: str = field(compare=False)
    satoshi_balance: int
    blockchain_id: int = field(compare=False)
    decimals: int = field(compare=False, default=8)
    balance: float = field(compare=False, default=None)

    def __post_init__(self):
        self.balance = from_satoshi(self.satoshi_balance, self.decimals)


metadata_obj = MetaData()

blockchain = Table(
    "blockchain",
    metadata_obj,
    Column("blockchain_id", Integer, primary_key=True, autoincrement=False),
    Column("name", String(32)),
    Column("short_name", String(16)),
    Column("short_name_lower", String(16)),
)

address_balance = Table(
    "address_balance",
    metadata_obj,
    Column("address", String(128)),
    Column("blockchain_id", Integer, ForeignKey("blockchain.blockchain_id")),
    Column("satoshi_balance", BigInteger),
    Column("decimals", Integer),
    Column("balance", DECIMAL(18, 10)),
    PrimaryKeyConstraint('address', 'blockchain_id', name='address_balance_pk')
)

excluded_address = Table(
    "excluded_address",
    metadata_obj,
    Column("value", String(128), primary_key=True),
    Column("blockchain_id", Integer, ForeignKey("blockchain.blockchain_id")),
)

mapper_registry.map_imperatively(Blockchain, blockchain, properties={
    'excluded_addresses': relationship(ExcludedAddress, backref='blockchain'),
    "addresses_balances": relationship(AddressBalance, backref="blockchain")
})

mapper_registry.map_imperatively(ExcludedAddress, excluded_address)

mapper_registry.map_imperatively(AddressBalance, address_balance)