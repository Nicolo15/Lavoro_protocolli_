namespace NetCoreClient.ValueObjects
{
    internal class WaterTemperature
    {
        public int Value { get; private set; }

        public WaterTemperature(int value)
        {
            this.Value = value;
        }

    }
}