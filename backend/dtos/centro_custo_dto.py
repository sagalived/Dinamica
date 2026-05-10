from dataclasses import dataclass


@dataclass(frozen=True)
class CentroCustoFiltrosDTO:
    company_id: str | None = None
    building_id: str | None = None
    company_id_alias: str | None = None
    building_id_alias: str | None = None

    @property
    def resolved_company_id(self) -> str | None:
        return self.company_id or self.company_id_alias

    @property
    def resolved_building_id(self) -> str | None:
        return self.building_id or self.building_id_alias
