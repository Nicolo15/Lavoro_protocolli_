namespace NetCoreClient.ValueObjects
{
    internal class Light
    {
        public int Value { get; private set; }

        public Light(int value)
        {
            this.Value = value;
        }

    }
}