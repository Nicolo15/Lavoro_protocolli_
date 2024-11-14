namespace NetCoreClient.ValueObjects
{
    internal class WaterData
    {
        public string Id { get; set; }              // Campo "id" per identificare il dispositivo
        public string CoolerId { get; set; }
        public int LitersDispensed { get; private set; }
        public int WaterTemperature { get; private set; }
        public string FilterStatus { get; private set; }
        public bool NightLight { get; private set; }
        public bool MaintenanceMode { get; private set; }

        public WaterData(int litersDispensed, int waterTemperature, string filterStatus, bool nightLight, bool maintenanceMode)
        {
            this.Id = Id;
            this.CoolerId = CoolerId;
            this.LitersDispensed = litersDispensed;
            this.WaterTemperature = waterTemperature;
            this.FilterStatus = filterStatus;
            this.NightLight = nightLight;
            this.MaintenanceMode = maintenanceMode;
        }
    }
}


