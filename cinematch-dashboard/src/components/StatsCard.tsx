import './StatsCard.css';

interface StatsCardProps {
    icon: string;
    label: string;
    value: string | number;
    subtitle?: string;
    trend?: 'up' | 'down' | 'neutral';
    trendValue?: string;
}

export function StatsCard({ icon, label, value, subtitle, trend, trendValue }: StatsCardProps) {
    return (
        <div className="stats-card">
            <div className="stats-card-icon">{icon}</div>
            <div className="stats-card-content">
                <span className="stats-card-label">{label}</span>
                <span className="stats-card-value">{value}</span>
                {subtitle && <span className="stats-card-subtitle">{subtitle}</span>}
                {trend && trendValue && (
                    <span className={`stats-card-trend trend-${trend}`}>
                        {trend === 'up' ? '↑' : trend === 'down' ? '↓' : '→'} {trendValue}
                    </span>
                )}
            </div>
        </div>
    );
}
